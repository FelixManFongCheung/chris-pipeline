FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py chris_email.py scrape.py orchestrate.py ./
# GeoJSON for scrape.py (must not be excluded by .dockerignore *.json)
COPY georef-united-states-of-america-zc-point.json ./
RUN test -f /app/georef-united-states-of-america-zc-point.json
CMD ["python", "orchestrate.py"]
