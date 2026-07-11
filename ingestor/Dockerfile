FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# deps before code so the layer stays cached between builds
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestor.py .

RUN useradd --create-home --shell /usr/sbin/nologin worker
USER worker

STOPSIGNAL SIGTERM
CMD ["python", "ingestor.py"]
