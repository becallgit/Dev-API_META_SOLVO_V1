# Use an official Python runtime as a parent image
FROM python:3.12.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install cron
RUN apt-get update && apt-get install -y cron

# Create a crontab file to run the script every 30 minutes
RUN (crontab -l 2>/dev/null; echo "*/30 * * * * cd /app && python RUN.py >> /app/cron.log 2>&1") | crontab -

# Ensure the cron daemon runs
CMD ["cron", "-f"]
