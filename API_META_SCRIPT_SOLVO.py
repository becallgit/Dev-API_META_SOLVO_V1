"""
API_META_SCRIPT_SOLVO.py
ETL de Meta Ads adaptado al esquema Solvo
"""

import mysql.connector
import logging
from mysql.connector import Error
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.exceptions import FacebookRequestError
from decimal import Decimal
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from dateutil.parser import parse
import json
import time , random


# ==============================
# 🔹 Carga de variables de entorno
# ==============================
load_dotenv()

DB_HOST     = os.getenv("ORG_DB_HOST", "127.0.0.1")
DB_USER     = os.getenv("ORG_DB_USER")
DB_PASSWORD = os.getenv("ORG_DB_PASSWORD")
DB_NAME     = os.getenv("ORG_DB_NAME")

DECRYPTION_KEY = os.getenv("DECRYPTION_KEY")

RETRYABLE_CODES = {17, 613}            # throttling típicos
RETRYABLE_SUBCODES = {2446079, 1487742}
PLATFORM_KEY = 'Meta'  # o 'meta', pero usa SIEMPRE el mismo valor


logger = logging.getLogger(__name__)

def throttle_guard(func):
    def wrapper(*args, **kwargs):
        delay = 60  # base
        for attempt in range(6):  # hasta ~1h exponencial
            try:
                return func(*args, **kwargs)
            except FacebookRequestError as e:
                code = e.api_error_code()
                sub  = e.api_error_subcode()
                if (code in RETRYABLE_CODES) or (sub in RETRYABLE_SUBCODES):
                    sleep_s = delay * (2 ** attempt) + random.uniform(0, 5)
                    logger.warning(f"⏳ Throttle (code={code}, sub={sub}). Reintentando en {int(sleep_s)}s…")
                    time.sleep(sleep_s)
                    continue
                raise
        logger.error("❌ Máximo de reintentos por throttling alcanzado.")
        return None
    return wrapper

# ==============================
# 🔹 Conexión a la base de datos
# ==============================

def connect_to_database():
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            conn = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                autocommit=False
            )
            if conn.is_connected():
                logger.info(f"✅ Conexión establecida (intento {attempt}).")
                return conn
        except Error as e:
            logger.error(f"❌ Intento {attempt}: {e}")
        time.sleep(1)
    logger.error("❌ No se pudo conectar tras 5 intentos.")
    return None

# ==============================
# 🔹 Credenciales de la API
# ==============================

def get_api_credentials(platform: str, key: str):
    connection = connect_to_database()
    if not connection:
        raise Error("No se pudo conectar a la base de datos para obtener credenciales.")

    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT CONVERT(AES_DECRYPT(config, %s) USING utf8)
            FROM api_credentials_solvo
            WHERE platform = %s
            """,
            (key, platform)
        )
        result = cursor.fetchone()
        if result and result[0]:
            logger.info("🔑 Credenciales obtenidas y desencriptadas.")
            return json.loads(result[0])
        raise ValueError(f"No se encontraron credenciales para la plataforma: {platform}")
    finally:
        cursor.close()
        connection.close()

meta_config = get_api_credentials("meta", DECRYPTION_KEY)
API_VERSION = meta_config.get("META_API_VERSION", "v23.0")
AD_ACCOUNT_IDS = meta_config.get("META_AD_ACCOUNT_IDS") or []

# compat: si viniera solo una cuenta en la clave antigua
if not AD_ACCOUNT_IDS and meta_config.get("META_AD_ACCOUNT_ID"):
    AD_ACCOUNT_IDS = [meta_config["META_AD_ACCOUNT_ID"]]

if not AD_ACCOUNT_IDS:
    raise ValueError("No hay cuentas de anuncios configuradas en META_AD_ACCOUNT_IDS.")


# ==============================
# 🔹 Inicialización de la API
# ==============================

def initialize_meta_api():
    try:
        FacebookAdsApi.init(
            meta_config["META_APP_ID"],
            meta_config["META_APP_SECRET"],
            meta_config["META_ACCESS_TOKEN"],
            api_version=API_VERSION  # usar versión explícita
        )
        logger.info(f"✅ Meta API inicializada (version {API_VERSION}).")
    except Exception as e:
        logger.error(f"❌ Error al inicializar Meta API: {e}")
        raise


# ==============================
# 🔹 Extracción de datos
# ==============================

@throttle_guard
def fetch_campaign_data(ad_account_id: str, last_sync_epoch: int | None = None):
    """
    Devuelve campañas del ad account con menos llamadas (limit alto, filtro de estado)
    y permite incremental por updated_time si pasas last_sync_epoch (UNIX seconds).
    """
    account = AdAccount(ad_account_id)
    fields = [
        Campaign.Field.id,
        Campaign.Field.name,
        Campaign.Field.objective,
        Campaign.Field.status,
        Campaign.Field.effective_status,
        Campaign.Field.start_time,
        Campaign.Field.stop_time,
        Campaign.Field.updated_time,
    ]

    params = {
        'limit': 500,
        'effective_status': ['ACTIVE', 'PAUSED', 'WITH_ISSUES'],
    }

    if last_sync_epoch:
        params['filtering'] = [{
            'field': 'campaign.updated_time',
            'operator': 'GREATER_THAN',
            'value': int(last_sync_epoch)
        }]

    try:
        campaigns = account.get_campaigns(fields=fields, params=params)
        result = []
        for c in campaigns:
            result.append({
                'campaign_id': c.get('id'),
                'campaign_name': c.get('name'),
                'objective': c.get('objective'),
                'status': c.get('status'),
                'effective_status': c.get('effective_status'),
                'start_date': parse(c['start_time']).strftime('%Y-%m-%d') if c.get('start_time') else None,
                'end_date':   parse(c['stop_time']).strftime('%Y-%m-%d')  if c.get('stop_time')  else None,
                'updated_time': c.get('updated_time'),
                'ad_account_id': ad_account_id,   # 👈 AÑADIDO: rellena con la cuenta del bucle
            })
        logger.info(f"📊 Campañas ({ad_account_id}): {len(result)}")
        return result
    except FacebookRequestError as e:
        logger.error(f"❌ Campañas ({ad_account_id}) error {e.api_error_code()}/{e.api_error_subcode()}: {e.api_error_message()}")
        raise
    except Exception as e:
        logger.exception(f"❌ Campañas ({ad_account_id}) fallo inesperado: {e}")
        return None


@throttle_guard
def fetch_adset_data(ad_account_id: str, last_sync_epoch: int | None = None):
    """
    Devuelve ad sets del ad account con limit alto y filtro de estado.
    Soporta incremental por updated_time si pasas last_sync_epoch.
    """
    account = AdAccount(ad_account_id)
    fields = [
        AdSet.Field.id,
        AdSet.Field.name,
        AdSet.Field.campaign_id,
        AdSet.Field.status,
        AdSet.Field.effective_status,
        AdSet.Field.start_time,
        AdSet.Field.end_time,
        AdSet.Field.daily_budget,
        AdSet.Field.lifetime_budget,
        AdSet.Field.updated_time,
    ]

    params = {
        'limit': 500,
        'effective_status': ['ACTIVE', 'PAUSED', 'WITH_ISSUES'],
    }

    if last_sync_epoch:
        params['filtering'] = [{
            'field': 'adset.updated_time',
            'operator': 'GREATER_THAN',
            'value': int(last_sync_epoch)
        }]

    try:
        adsets = account.get_ad_sets(fields=fields, params=params)
        result = []
        for a in adsets:
            result.append({
                'adset_id': a.get('id'),
                'adset_name': a.get('name'),
                'campaign_id': a.get('campaign_id'),
                'status': a.get('status'),
                'effective_status': a.get('effective_status'),
                'start_date': parse(a['start_time']).strftime('%Y-%m-%d') if a.get('start_time') else None,
                'end_date':   parse(a['end_time']).strftime('%Y-%m-%d')   if a.get('end_time')   else None,
                'daily_budget':   int(a.get('daily_budget'))    if a.get('daily_budget')    else None,
                'lifetime_budget': int(a.get('lifetime_budget')) if a.get('lifetime_budget') else None,
                'updated_time': a.get('updated_time'),
            })
        logger.info(f"📊 AdSets ({ad_account_id}): {len(result)}")
        return result
    except FacebookRequestError as e:
        logger.error(f"❌ AdSets ({ad_account_id}) error {e.api_error_code()}/{e.api_error_subcode()}: {e.api_error_message()}")
        raise
    except Exception as e:
        logger.exception(f"❌ AdSets ({ad_account_id}) fallo inesperado: {e}")
        return None

@throttle_guard
def fetch_ad_data(ad_account_id: str, last_sync_epoch: int | None = None):
    account = AdAccount(ad_account_id)
    fields  = ['id', 'name', 'adset_id', 'status', 'effective_status', 'updated_time']
    params  = {
        'limit': 500,
        'effective_status': ['ACTIVE', 'PAUSED', 'WITH_ISSUES', 'ARCHIVED'],
    }
    if last_sync_epoch:
        params['filtering'] = [{
            'field': 'ad.updated_time',
            'operator': 'GREATER_THAN',
            'value': int(last_sync_epoch)
        }]

    ads = account.get_ads(fields=fields, params=params)
    logger.info(f"📊 Ads ({ad_account_id}): {len(ads)}")
    return [
        {
            'ad_id': ad.get('id'),
            'ad_name': ad.get('name'),
            'adset_id': ad.get('adset_id'),
            'status': ad.get('status'),
            'effective_status': ad.get('effective_status'),
            'updated_time': ad.get('updated_time'),
        } for ad in ads
    ]

@throttle_guard
def fetch_account_data(ad_account_id: str):
    # Campos válidos en v23.0
    fields = [
        'id','account_id','name','account_status','currency',
        'timezone_id','timezone_name','timezone_offset_hours_utc',
        'amount_spent','spend_cap','balance',
        'business','created_time'
    ]
    acc = AdAccount(ad_account_id).api_get(fields=fields)
    biz = acc.get('business') or {}

    amount_spent = acc.get('amount_spent')
    spend_cap    = acc.get('spend_cap')

    try:
        remaining_cap = (Decimal(spend_cap) - Decimal(amount_spent)) if (spend_cap is not None and amount_spent is not None) else None
    except Exception:
        remaining_cap = None

    created = acc.get('created_time')
    created_sql = parse(created).strftime('%Y-%m-%d %H:%M:%S') if created else None

    return [{
        'ad_account_id': acc.get('id'),                    # act_...
        'account_id_numeric': acc.get('account_id'),       # numérico
        'name': acc.get('name'),
        'account_status': acc.get('account_status'),
        'currency': acc.get('currency'),
        'timezone_id': acc.get('timezone_id'),
        'timezone_name': acc.get('timezone_name'),
        'timezone_offset_utc': acc.get('timezone_offset_hours_utc'),
        'amount_spent': float(amount_spent) if amount_spent is not None else None,
        'spend_cap': float(spend_cap) if spend_cap is not None else None,
        'remaining_cap': float(remaining_cap) if remaining_cap is not None else None,
        'balance': float(acc.get('balance')) if acc.get('balance') is not None else None,
        'business_id': (biz.get('id') if isinstance(biz, dict) else None),
        'created_time': created_sql
    }]

def fetch_daily_metrics(ad_account_id: str):
    try:
        account = AdAccount(ad_account_id)
        fields = [
            AdsInsights.Field.ad_id,
            AdsInsights.Field.impressions,
            AdsInsights.Field.clicks,
            AdsInsights.Field.spend,
            AdsInsights.Field.reach,
            AdsInsights.Field.date_start
        ]
        today = datetime.now()
        last_week = today - timedelta(days=7)
        params = {
            'time_range': {'since': last_week.strftime('%Y-%m-%d'), 'until': today.strftime('%Y-%m-%d')},
            'time_increment': 1,
            'level': 'ad'
        }
        insights = account.get_insights(fields=fields, params=params)
        logger.info(f"📊 Métricas ({ad_account_id}): {len(insights)} filas")
        return [
            {
                'ad_id': i['ad_id'],
                'date_id': i['date_start'],
                'impressions': int(i.get('impressions', 0)),
                'clicks': int(i.get('clicks', 0)),
                'spend': float(i.get('spend', 0.0)),
                'reach': int(i.get('reach', 0)) if i.get('reach') else None
            }
            for i in insights
        ]
    except Exception as e:
        logger.error(f"❌ Métricas diarias ({ad_account_id}): {e}")
        return None

# ==============================
# 🔹 Helpers de carga
# ==============================

def ensure_status(connection, status_name):
    try:
        cur = connection.cursor()
        cur.execute(
            "SELECT status_id FROM dim_meta_status_solvo WHERE status_name = %s",
            (status_name,)
        )
        if not cur.fetchone():
            cur.execute(
                "INSERT INTO dim_meta_status_solvo (status_name) VALUES (%s)",
                (status_name,)
            )
            connection.commit()
        cur.execute(
            "SELECT status_id FROM dim_meta_status_solvo WHERE status_name = %s",
            (status_name,)
        )
        result = cur.fetchone()
        return result[0] if result else None
    except Error as e:
        logger.error(f"❌ ensure_status: {e}")
        return None
    finally:
        cur.close()

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def ensure_status_bulk(connection, status_names: set[str]) -> dict[str, int]:
    """Inserta estados faltantes en bloque y devuelve {status_name: status_id}."""
    if not status_names:
        return {}
    cur = connection.cursor()
    try:
        # Inserta faltantes en bloque (ignora duplicados)
        cur.executemany(
            "INSERT IGNORE INTO dim_meta_status_solvo (status_name) VALUES (%s)",
            [(s,) for s in status_names]
        )
        connection.commit()

        # Recupera mapping en chunks para evitar listas enormes en el IN
        mapping = {}
        names_list = list(status_names)
        for part in chunked(names_list, 1000):
            ph = ",".join(["%s"] * len(part))
            cur.execute(
                f"SELECT status_name, status_id FROM dim_meta_status_solvo WHERE status_name IN ({ph})",
                part
            )
            for name, sid in cur.fetchall():
                mapping[name] = sid
        return mapping
    finally:
        cur.close()

def bulk_upsert_values(connection, table: str, columns: list[str], rows: list[tuple], update_cols: list[str], chunk_size: int = 500):
    """
    Upsert por lotes usando multi-row VALUES. Compatible con MySQL 5.7/8.0.
    Nota: si tu servidor es 8.0.20+, puedes cambiar a la sintaxis con alias (ver comentario).
    """
    if not rows:
        return
    cur = connection.cursor(prepared=True)  # prepared puede acelerar el binding de parámetros
    try:
        col_list = ",".join(columns)
        one_row_ph = "(" + ",".join(["%s"] * len(columns)) + ")"
        # ON DUPLICATE con VALUES(): válido en 5.7/8.0 (deprecado desde 8.0.20, pero funciona)
        update_clause = ",".join([f"{c}=VALUES({c})" for c in update_cols])

        for part in chunked(rows, chunk_size):
            values_clause = ",".join([one_row_ph] * len(part))
            sql = f"INSERT INTO {table} ({col_list}) VALUES {values_clause} ON DUPLICATE KEY UPDATE {update_clause}"
            flat_params = [p for row in part for p in row]
            cur.execute(sql, flat_params)
        connection.commit()
    finally:
        cur.close()

    # 👉 Si quieres evitar el aviso deprecado en 8.0.20+, usa:
    # sql = f"INSERT INTO {table} ({col_list}) VALUES {values_clause} AS new ON DUPLICATE KEY UPDATE " + ",".join([f"{c}=new.{c}" for c in update_cols])
    # (Necesita MySQL 8.0.20+). :contentReference[oaicite:2]{index=2}

def load_general_data(data, table, connection):
    """
    Upserts por lotes.
    - dim_meta_account_solvo: upsert directo por lotes (no usa ensure_status).
    - dim_meta_campaign_solvo / dim_meta_adset_solvo / dim_meta_ad_solvo:
      resuelve status en bloque y upsert por lotes.
    """
    if not data:
        logger.warning(f"⚠ Sin datos para {table}.")
        return

    # ── DIM de CUENTAS ────────────────────────────────────────────────────────
    if table == 'dim_meta_account_solvo':
        rows = []
        for r in data:
            rows.append((
                r.get('ad_account_id'),
                r.get('account_id_numeric'),
                r.get('name'),
                r.get('account_status'),
                r.get('currency'),
                r.get('timezone_id'),
                r.get('timezone_name'),
                r.get('timezone_offset_utc'),
                r.get('amount_spent'),
                r.get('spend_cap'),
                r.get('remaining_cap'),
                r.get('balance'),
                r.get('business_id'),
                r.get('created_time'),
            ))
        bulk_upsert_values(
            connection,
            table='dim_meta_account_solvo',
            columns=['ad_account_id','account_id_numeric','name','account_status','currency',
                     'timezone_id','timezone_name','timezone_offset_utc',
                     'amount_spent','spend_cap','remaining_cap','balance',
                     'business_id','created_time'],
            rows=rows,
            update_cols=['account_id_numeric','name','account_status','currency',
                         'timezone_id','timezone_name','timezone_offset_utc',
                         'amount_spent','spend_cap','remaining_cap','balance',
                         'business_id','created_time'],
            chunk_size=500
        )
        logger.info("✅ Carga completada en dim_meta_account_solvo.")
        return

    # ── Otras DIMs (usa status en bloque) ────────────────────────────────────
    # 1) Resolver status en bloque
    statuses = set()
    for r in data:
        statuses.add(r.get('status') or r.get('effective_status') or 'UNKNOWN')
    status_map = ensure_status_bulk(connection, statuses)

    # 2) Preparar filas según tabla
    if table == 'dim_meta_campaign_solvo':
        rows = []
        for r in data:
            st = r.get('status') or r.get('effective_status') or 'UNKNOWN'
            rows.append((
                r.get('campaign_id'),
                r.get('campaign_name'),
                st,
                r.get('objective'),
                r.get('start_date'),
                r.get('end_date'),
                status_map.get(st),
                r.get('ad_account_id'), 
            ))
        bulk_upsert_values(
            connection,
            table='dim_meta_campaign_solvo',
            columns=['campaign_id','campaign_name','status','objective','start_date','end_date','status_id','ad_account_id'],
            rows=rows,
            update_cols=['campaign_name','status','objective','start_date','end_date','status_id','ad_account_id'],
            chunk_size=500
        )

    elif table == 'dim_meta_adset_solvo':
        rows = []
        for r in data:
            st = r.get('status') or r.get('effective_status') or 'UNKNOWN'
            rows.append((
                r.get('adset_id'),
                r.get('adset_name'),
                r.get('campaign_id'),
                st,
                status_map.get(st)
            ))
        bulk_upsert_values(
            connection,
            table='dim_meta_adset_solvo',
            columns=['adset_id','adset_name','campaign_id','status','status_id'],
            rows=rows,
            update_cols=['adset_name','campaign_id','status','status_id'],
            chunk_size=500
        )

    elif table == 'dim_meta_ad_solvo':
        rows = []
        for r in data:
            st = r.get('status') or r.get('effective_status') or 'UNKNOWN'
            rows.append((
                r.get('ad_id'),
                r.get('ad_name'),
                r.get('adset_id'),
                st,
                status_map.get(st)
            ))
        bulk_upsert_values(
            connection,
            table='dim_meta_ad_solvo',
            columns=['ad_id','ad_name','adset_id','status','status_id'],
            rows=rows,
            update_cols=['ad_name','adset_id','status','status_id'],
            chunk_size=500
        )

    else:
        raise ValueError(f"Tabla no soportada en load_general_data: {table}")

    logger.info(f"✅ Carga completada en {table}.")

def load_fact_meta_ad(data, connection):
    """
    Upsert masivo de métricas (PK = ad_id, date_id) sin SELECT previo.
    """
    if not data:
        logger.warning("⚠ Sin métricas para fact_meta_ad_solvo.")
        return

    rows = []
    for r in data:
        rows.append((
            r['ad_id'],
            r['date_id'],
            int(r.get('impressions') or 0),
            int(r.get('clicks') or 0),
            float(r.get('spend') or 0.0),
            (int(r.get('reach')) if r.get('reach') is not None else None),
        ))

    bulk_upsert_values(
        connection,
        table='fact_meta_ad_solvo',
        columns=['ad_id','date_id','impressions','clicks','spend','reach'],
        rows=rows,
        update_cols=['impressions','clicks','spend','reach'],
        chunk_size=500
    )
    logger.info("✅ Carga completada en fact_meta_ad_solvo.")

# ==============================
# 🔹 Tabla de control de update
# ==============================

def get_last_update(connection):
    cur = connection.cursor()
    try:
        cur.execute("SELECT last_update FROM time_update_solvo WHERE platform = %s", (PLATFORM_KEY,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()

def set_last_update(connection):
    cur = connection.cursor()
    try:
        # Delegamos el timestamp a MySQL (NOW())
        cur.execute(
            """
            INSERT INTO time_update_solvo (platform, last_update)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE last_update = NOW()
            """,
            (PLATFORM_KEY,)
        )
        connection.commit()

        # Verificación inmediata para log
        cur.execute("SELECT last_update FROM time_update_solvo WHERE platform = %s", (PLATFORM_KEY,))
        ts = cur.fetchone()
        logger.info(f"🕒 Última actualización registrada: {ts[0] if ts else 'NULL'}")
    except Exception as e:
        # No silenciar: queremos ver por qué no se guarda
        logger.error(f"❌ set_last_update falló: {e}")
        raise
    finally:
        cur.close()


def backfill_campaign_account_ids(ad_account_ids: list[str], connection):
    """
    Rellena dim_meta_campaign_solvo.ad_account_id para campañas ya existentes,
    sin pedir todos los campos (rápido).
    Trae campañas por cuenta (incluye ARCHIVED) y upsertea solo (campaign_id, ad_account_id).
    """
    from facebook_business.adobjects.adaccount import AdAccount

    pairs = []  # (campaign_id, ad_account_id)
    for acc in ad_account_ids:
        try:
            account = AdAccount(acc)
            # Sólo 'id' + limit alto para minimizar páginas;
            # incluye ARCHIVED para cubrir históricas.
            camps = account.get_campaigns(
                fields=['id'],
                params={'limit': 500, 'effective_status': ['ACTIVE','PAUSED','WITH_ISSUES','ARCHIVED']}
            )
            for c in camps:
                cid = c.get('id')
                if cid:
                    pairs.append((cid, acc))
            logger.info(f"🔁 Backfill campañas → {acc}: {len(pairs)} pares acumulados")
        except Exception as e:
            logger.warning(f"Backfill campañas {acc}: {e}")

    if not pairs:
        logger.info("Backfill campañas: no hay pares que upsertear.")
        return

    # Upsert masivo sólo de (campaign_id, ad_account_id)
    bulk_upsert_values(
        connection,
        table='dim_meta_campaign_solvo',
        columns=['campaign_id','ad_account_id'],
        rows=pairs,
        update_cols=['ad_account_id'],
        chunk_size=500
    )
    logger.info(f"✅ Backfill de ad_account_id en campañas: {len(pairs)} filas intentadas")

# ==============================
# 🔹 Función principal
# ==============================

def main() -> bool:
    """
    ETL multi-cuenta para Meta Ads.
    - Lee credenciales cifradas (incluye META_AD_ACCOUNT_IDS).
    - Inicializa el SDK con api_version.
    - (Opcional) Extrae información de la cuenta (dimensión de cuentas).
    - Extrae campañas, ad sets, ads e insights día a día por cuenta.
    - Usa rango incremental desde la última sync con pequeño solape.
    - Carga en tablas destino.
    """
    logger = logging.getLogger("meta_etl")
    logger.setLevel(logging.INFO)

    # 1) Credenciales
    load_dotenv()
    dec_key = os.getenv("DECRYPTION_KEY")
    if not dec_key:
        logger.error("DECRYPTION_KEY no definido en el entorno (.env).")
        return False

    try:
        meta_cfg = get_api_credentials("meta", dec_key)
    except Exception as e:
        logger.error(f"No se pudieron obtener credenciales META: {e}")
        return False

    api_version    = meta_cfg.get("META_API_VERSION", "v23.0")
    ad_account_ids = meta_cfg.get("META_AD_ACCOUNT_IDS") or []
    if not ad_account_ids and meta_cfg.get("META_AD_ACCOUNT_ID"):
        ad_account_ids = [meta_cfg["META_AD_ACCOUNT_ID"]]

    if not ad_account_ids:
        logger.error("No hay cuentas configuradas en META_AD_ACCOUNT_IDS.")
        return False

    # 2) Inicializar SDK (acepta api_version)
    try:
        FacebookAdsApi.init(
            meta_cfg["META_APP_ID"],
            meta_cfg["META_APP_SECRET"],
            meta_cfg["META_ACCESS_TOKEN"],
            api_version=api_version
        )
        logger.info(f"SDK de Meta inicializado (Marketing API {api_version}).")
    except Exception as e:
        logger.error(f"Error al inicializar SDK de Meta: {e}")
        return False

    # 3) Conexión a BBDD
    connection = connect_to_database()
    if not connection:
        logger.error("No se pudo abrir conexión MySQL.")
        return False

    try:
        # 3.1 Rango incremental para insights (última sync con solape)
        #     Si no hay registro previo, toma últimos 14 días.
        today = datetime.now().date()
        since_date = today - timedelta(days=14)
        try:
            last_sync = get_last_update(connection)  # puede devolver None
            logger.info(f"Última sincronización registrada: {last_sync}")
            if last_sync:
                since_date = (last_sync.date() - timedelta(days=2))  # solape 2 días
                if since_date > today:
                    since_date = today
        except Exception:
            logger.info("No se pudo obtener last_update (continuamos con 14 días por defecto).")

        since_str = since_date.strftime('%Y-%m-%d')
        until_str = today.strftime('%Y-%m-%d')
        # Epoch para filtros incrementales en campaign/adset/ad (updated_time)
        last_sync_epoch = None
        try:
            if last_sync:
                last_sync_epoch = int(last_sync.timestamp())
        except Exception:
            pass

        all_accounts, all_campaigns, all_adsets, all_ads, all_metrics = [], [], [], [], []

        # 4) Loop por cuentas
        for acc in ad_account_ids:
            logger.info(f"➡️  Procesando cuenta {acc}")
            try:
                # 4.1 (Opcional) Dimensión de cuentas si definiste fetch_account_data()
                try:
                    if 'fetch_account_data' in globals():
                        a = fetch_account_data(acc) or []
                        all_accounts += a
                except NameError:
                    pass
                except Exception as e:
                    logger.warning(f"Cuenta {acc}: no se pudo leer info de cuenta ({e}).")

                # 4.2 Campañas / AdSets / Ads con soporte incremental si tu fetch_* lo acepta
                try:
                    c = fetch_campaign_data(acc, last_sync_epoch)  # nueva firma
                except TypeError:
                    c = fetch_campaign_data(acc)  # fallback a firma antigua
                except Exception as e:
                    logger.exception(f"Campañas {acc}: {e}")
                    c = []

                try:
                    s = fetch_adset_data(acc, last_sync_epoch)  # nueva firma
                except TypeError:
                    s = fetch_adset_data(acc)  # fallback a firma antigua
                except Exception as e:
                    logger.exception(f"AdSets {acc}: {e}")
                    s = []

                try:
                    a = fetch_ad_data(acc, last_sync_epoch)  # nueva firma
                except TypeError:
                    a = fetch_ad_data(acc)  # fallback a firma antigua
                except Exception as e:
                    logger.exception(f"Ads {acc}: {e}")
                    a = []

                # 4.3 Insights (diarios) con rango since/until si tu fetch lo acepta
                try:
                    m = fetch_daily_metrics(acc, since_str, until_str)  # firma nueva
                except TypeError:
                    m = fetch_daily_metrics(acc)  # fallback a firma antigua (últimos 7 días hardcodeados)
                except Exception as e:
                    logger.exception(f"Métricas {acc}: {e}")
                    m = []

                all_campaigns += (c or [])
                all_adsets    += (s or [])
                all_ads       += (a or [])
                all_metrics   += (m or [])

                logger.info(
                    f"   OK {acc}: {len(c or [])} campañas, {len(s or [])} adsets, "
                    f"{len(a or [])} ads, {len(m or [])} métricas"
                )
            except Exception as e:
                # seguimos con el resto de cuentas
                logger.exception(f"Error al procesar {acc}: {e}")

        if not (all_accounts or all_campaigns or all_adsets or all_ads or all_metrics):
            logger.error("Extracción vacía en todas las cuentas.")
            return False

        # 5) Cargas (con try/except por si alguna tabla aún no existe)
        try:
            if all_accounts:
                load_general_data(all_accounts, 'dim_meta_account_solvo', connection)
        except Exception as e:
            logger.warning(f"dim_meta_account_solvo: no se cargó ({e}). ¿Creaste la tabla?")

        load_general_data(all_campaigns, 'dim_meta_campaign_solvo', connection)
        load_general_data(all_adsets,    'dim_meta_adset_solvo',   connection)
        load_general_data(all_ads,       'dim_meta_ad_solvo',      connection)
        load_fact_meta_ad(all_metrics,   connection)
        # backfill_campaign_account_ids(ad_account_ids, connection)
        # 6) Marca de tiempo de sync (si tienes set_last_update)
        set_last_update(connection)  # si falla, queremos ver el error
        

        logger.info("Carga completada ✅")
        return True

    finally:
        try:
            connection.close()
            logger.debug("Conexión MySQL cerrada.")
        except Exception:
            pass


# ==============================
# 🔹 Ejecución directa
# ==============================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    main()

