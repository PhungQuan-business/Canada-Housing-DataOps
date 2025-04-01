FROM quay.io/astronomer/astro-runtime:12.7.1
# ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow"
ENV PYTHONPATH "/usr/local/airflow/canada-house:${PYTHONPATH}"