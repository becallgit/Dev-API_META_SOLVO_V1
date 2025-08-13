FROM python:3.12.8-slim

# Logs sin buffer + zona horaria
ENV PYTHONUNBUFFERED=1 TZ=Europe/Madrid

WORKDIR /app

# Copiar código y dependencias
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

# Instalar cron y bash
RUN apt-get update && apt-get install -y cron bash && rm -rf /var/lib/apt/lists/*

# Asegurar fichero de log
RUN touch /app/cron.log

# Copiar variables de entorno actuales al fichero que cargará cron
# Esto se ejecuta en build, pero las variables de docker-compose se añaden en runtime.
# Por eso, lo importante es que la línea de cron las cargue en runtime.
# Añadimos /etc/environment vacío ahora por si se quiere rellenar en entrypoint.
RUN touch /etc/environment

# Añadir crontab con carga de /etc/environment
RUN (crontab -l 2>/dev/null; \
     echo "*/30 * * * * . /etc/environment && echo \"\$(date '+\%Y-\%m-\%d \%H:\%M:\%S') - Ejecutando RUN.py\" >> /app/cron.log 2>&1 && cd /app && /usr/local/bin/python -u RUN.py >> /app/cron.log 2>&1") | crontab -

# Entrypoint que guarda las variables en /etc/environment en cada arranque
# Esto asegura que las variables de docker-compose estén disponibles para cron
RUN echo '#!/bin/bash\nprintenv | grep -v "no_proxy" > /etc/environment\ncron -f' > /entrypoint.sh \
    && chmod +x /entrypoint.sh

# Usar entrypoint como proceso principal
CMD ["/entrypoint.sh"]
