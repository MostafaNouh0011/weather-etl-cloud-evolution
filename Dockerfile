FROM apache/airflow:2.10.3

# Switch to root only for system-level installs (none needed here)
USER airflow

# Copy and install Python dependencies at build time.
# This is the correct production approach — packages are baked into the image
# rather than installed at container startup via _PIP_ADDITIONAL_REQUIREMENTS.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt