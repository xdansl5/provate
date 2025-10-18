# Usiamo il tag v3.5.1 che è disponibile
FROM apache/spark-py:latest

# 1. Passa temporaneamente a ROOT per avere i permessi
USER root

# 2. Copia e installa i requisiti Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ==========================================================
# SEZIONE AGGIUNTA (IL FIX)
# ==========================================================
# 3. Create necessary directories and set permissions
RUN mkdir -p /opt/spark/.ivy2 && \
    mkdir -p /tmp/checkpoints/logs/rules && \
    mkdir -p /tmp/delta-lake/logs && \
    mkdir -p /tmp/delta-lake/ml-predictions && \
    mkdir -p /tmp/ml_models && \
    chown -R 185:0 /opt/spark/.ivy2 && \
    chown -R 185:0 /tmp/checkpoints && \
    chown -R 185:0 /tmp/delta-lake && \
    chown -R 185:0 /tmp/ml_models && \
    chmod -R 777 /tmp/checkpoints && \
    chmod -R 777 /tmp/delta-lake && \
    chmod -R 777 /tmp/ml_models
# ==========================================================

# 4. Torna all'utente SPARK non-privilegiato
USER 185

# 5. Copiamo il resto del codice del progetto
COPY . /app