# Use the official Apache Spark image with Python support (PySpark).
# Pinning the version to v3.4.0 ensures reproducible builds.

FROM apache/spark-py:v3.4.0

# 1. Temporarily switch to the root user to gain privileges
#    for package installation and directory management.
USER root

# 2. Copy the Python requirements file and install dependencies.
#    --no-cache-dir is used to reduce the final image size.
COPY scripts/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Create all necessary directories for Spark and the application.
#    This is done as root before switching back to the non-privileged user.
#    We chain commands in a single RUN layer to optimize image size.
RUN mkdir -p /opt/spark/.ivy2 && \
    mkdir -p /tmp/checkpoints/logs/rules && \
    mkdir -p /tmp/delta-lake/logs && \
    mkdir -p /tmp/delta-lake/ml-predictions && \
    mkdir -p /tmp/ml_models && \
    # Change ownership to the non-root 'spark' user (UID 185) and group '0' (root).
    # This allows the Spark process to write to these directories at runtime
    # for package caching (.ivy2), streaming checkpoints, Delta tables, and ML models.
    chown -R 185:0 /opt/spark/.ivy2 && \
    chown -R 185:0 /tmp/checkpoints && \
    chown -R 185:0 /tmp/delta-lake && \
    chown -R 185:0 /tmp/ml_models && \
    # Set broad permissions (777: rwx for all).
    # This is a pragmatic fix to prevent permission errors, especially
    # when mounting host volumes.
    # A safer alternative, if possible, would be 775.
    chmod -R 777 /tmp/checkpoints && \
    chmod -R 777 /tmp/delta-lake && \
    chmod -R 777 /tmp/ml_models
# ==========================================================

# 4. Revert to the non-privileged 'spark' user (UID 185).
#    This is a critical security best practice to avoid running
#    the container application as root.
USER 185

# 5. Copy the entire project (application code, scripts, etc.)
#    into the /app directory. This is done after installing
#    dependencies to optimize Docker layer caching.
COPY . /app