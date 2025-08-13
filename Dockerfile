FROM python:3.12.8-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

# Instalar cron y bash
RUN apt-get update && apt-get install -y cron bash && rm -rf /var/lib/apt/lists/*

# Añadir crontab con log timestamp
RUN (crontab -l 2>/dev/null; \
     echo "*/30 * * * * echo \"\$(date '+\%Y-\%m-\%d \%H:\%M:\%S') - Ejecutando RUN.py\" >> /app/cron.log && cd /app && python RUN.py >> /app/cron.log 2>&1") | crontab -

CMD ["cron", "-f"]
