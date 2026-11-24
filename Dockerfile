FROM python:3.12-slim

ARG INSTALL_SIGNOZ_MCP=false
ARG SIGNOZ_MCP_VERSION=v0.1.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Optional: fetch signoz-mcp-server release binary when INSTALL_SIGNOZ_MCP=true
# Otherwise mount your binary to /usr/local/bin/signoz-mcp-server via docker-compose.yml
RUN if [ "$INSTALL_SIGNOZ_MCP" = "true" ]; then \
      ARCH="$(dpkg --print-architecture)" && \
      curl -fsSL "https://github.com/SigNoz/signoz-mcp-server/releases/download/${SIGNOZ_MCP_VERSION}/signoz-mcp-server_linux_${ARCH}.tar.gz" \
        | tar -xz -C /usr/local/bin/ signoz-mcp-server && \
      chmod +x /usr/local/bin/signoz-mcp-server; \
    fi

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ /app/

RUN useradd -m -u 1000 jarvis && mkdir -p /app/data && chown -R jarvis:jarvis /app
USER jarvis

EXPOSE 8765

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -sf http://localhost:8765/health || exit 1

CMD ["gunicorn", "--bind", "0.0.0.0:8765", "--workers", "1", "--threads", "4", \
     "--timeout", "600", "--access-logfile", "-", "server:app"]
