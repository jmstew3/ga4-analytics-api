FROM python:3.12.8-slim-bookworm

# Non-root user
RUN groupadd --system ga4group && \
    useradd --system --gid ga4group --no-create-home --shell /usr/sbin/nologin ga4user

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Own the working directory
RUN chown -R ga4user:ga4group /app

USER ga4user

ENTRYPOINT ["python", "-m", "app.main"]
