FROM python:3.9-slim

LABEL org.opencontainers.image.title="HadoopScope" \
      org.opencontainers.image.description="Unified Hadoop cluster health monitoring" \
      org.opencontainers.image.source="https://github.com/disoardi/hadoopscope"

WORKDIR /app

# PyYAML opzionale (il core gira senza)
RUN pip install --no-cache-dir pyyaml==6.0.1

COPY . /app/

# Togliere i file non necessari all'runtime
RUN rm -rf .git tests/__pycache__ checks/__pycache__ .claude

ENTRYPOINT ["python3", "hadoopscope.py"]
CMD ["--help"]
