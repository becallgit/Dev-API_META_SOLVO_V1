"""
RUN.py  ―  Lanzador y orquestador del ETL de Meta para Solvo
-------------------------------------------------------------
• Configura logging a archivo y consola
• Carga variables de entorno (dotenv)
• Ejecuta el ETL de Meta (API_META_SCRIPT_SOLVO.main)
• Envía correo si hay fallos y opcionalmente el log diario
"""

import logging
import os
import traceback
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from dotenv import load_dotenv

# ▶ Módulo ETL
from API_META_SCRIPT_SOLVO import main as meta_main

# =============================================================
# 🔹 Configuración inicial
# =============================================================

LOG_FILE = "execution_log.txt"

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

# =============================================================
# 🔹 Email helpers
# =============================================================

def send_email(subject: str, body: str):
    """Envía un email con el log adjunto."""
    try:
        sender_email    = os.getenv("SENDER_EMAIL")
        sender_password = os.getenv("SENDER_PASSWORD")
        recipient_email = os.getenv("RECIPIENT_EMAIL")

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "plain", "utf-8"))

        with open(LOG_FILE, "rb") as log_file:
            attachment = MIMEApplication(log_file.read(), _subtype="txt")
            attachment.add_header("Content-Disposition",
                                  "attachment",
                                  filename=LOG_FILE)
            msg.attach(attachment)

        smtp_server = "smtp.gmail.com"
        smtp_port   = 587
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()

        logging.info("📧 Email enviado correctamente.")
    except Exception as e:
        logging.error(f"❌ Error al enviar email: {e}")

def send_failure_email():
    send_email(
        subject="FALLO en proceso Meta Solvo",
        body="Se produjo un fallo durante la ejecución del ETL de Meta. "
             "Revisa el log adjunto."
    )

def send_daily_log_email():
    """Envía el log a una hora ventana (14:25–15:30) para control rutinario."""
    now        = datetime.now().time()
    start_time = datetime.strptime("14:25", "%H:%M").time()
    end_time   = datetime.strptime("15:30", "%H:%M").time()
    if start_time <= now <= end_time:
        send_email(
            subject="Log diario ETL Meta Solvo",
            body="Adjunto el log de ejecución del día."
        )

# =============================================================
# 🔹 Ejecución de la plataforma Meta
# =============================================================

def execute_meta() -> bool:
    """Lanza el ETL de Meta y devuelve True/False según éxito."""
    try:
        logging.info("🚀 [META] START")
        success = meta_main()
        if success:
            logging.info("✅ [META] SUCCESS")
        else:
            logging.error("❌ [META] ERROR")
        return success
    except Exception as e:
        logging.critical(f"💥 [META] CRITICAL: {e}\n{traceback.format_exc()}")
        return False

# =============================================================
# 🔹 Punto de entrada
# =============================================================

def main():
    setup_logging()
    load_dotenv()

    logging.info("-" * 90)
    logging.info(f"🏁 Inicio ejecución ETL Meta Solvo - {datetime.now()}")

    meta_ok = execute_meta()

    logging.info(f"📊 Resumen final → META: {'OK' if meta_ok else 'FAIL'}")

    if not meta_ok:
        send_failure_email()

    logging.info("-" * 90)

    # Enviar log rutinario en la franja horaria definida
    send_daily_log_email()

if __name__ == "__main__":
    main()
