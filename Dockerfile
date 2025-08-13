FROM python:3.12.8-slim

# Opcional: logs sin buffer + zona horaria
ENV PYTHONUNBUFFERED=1 TZ=Europe/Madrid

WORKDIR /app

# Copia el código y dependencias
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

# Instalar cron y bash
RUN apt-get update && apt-get install -y cron bash && rm -rf /var/lib/apt/lists/*

# Asegurar fichero de log (opcional)
RUN touch /app/cron.log

# Añadir crontab con timestamp y Python por ruta absoluta
# Nota: cron NO usa tu PATH de login; por eso usamos /usr/local/bin/python
RUN (crontab -l 2>/dev/null; \
     echo "*/30 * * * * echo \"\$(date '+\%Y-\%m-\%d \%H:\%M:\%S') - Ejecutando RUN.py\" >> /app/cron.log 2>&1 && cd /app && /usr/local/bin/python -u RUN.py >> /app/cron.log 2>&1") | crontab -

# Mantener cron en primer plano
CMD ["cron", "-f"]
