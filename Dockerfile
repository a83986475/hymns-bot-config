# ══════════════════════════════════════════
# Hymns Bot — Multi-stage Docker Build
# ══════════════════════════════════════════

# Stage 1: Node.js binary (only, not the full OS)
FROM node:24-slim AS node-stage

# Stage 2: Python runtime
FROM python:3.11-slim

# Copy Node.js binary from stage 1 (saves ~200MB vs full nodesource apt install)
COPY --from=node-stage /usr/local/bin/node /usr/local/bin/node

# Install system dependencies (minimal, no recommends)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source code
COPY . .

CMD ["python", "bot.py"]
