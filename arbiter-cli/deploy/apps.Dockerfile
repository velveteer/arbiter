# Shared image for the dummy producer and webhook handler. It adds only the
# OpenTelemetry SDK on top of python:slim; the scripts themselves are mounted, so
# they stay editable without a rebuild.
FROM python:3.12-slim
RUN pip install --no-cache-dir \
      opentelemetry-sdk==1.27.0 \
      opentelemetry-exporter-otlp-proto-http==1.27.0
WORKDIR /app
