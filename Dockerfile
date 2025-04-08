# integrates well with existing tools like Pandas for data manipulation and Boto3 for accessing cloud storage
# That's an old version – but our developer has experience with it, and it is compatible for 3rd party packages he uses.
FROM apache/airflow:1.10.10

USER root

# Old versions of Pandas and Boto3 are used for compatibility with legacy data systems
RUN pip install \
    pandas==0.24.0 \
    boto3==1.9.0 \
    requests \
    argcomplete

# Switch back to airflow user
USER airflow

# Copy DAGs into the image and Python Scripts
COPY ./dags /opt/airflow/dags

# The Airflow database is initialized, and example DAGs are loaded to service as templates
ENV AIRFLOW__CORE__LOAD_EXAMPLES=true
RUN airflow initdb

# Webserver – (UI) allowing users to view and manage their DAGs
# Scheduler – responsible for monitoring DAGs, triggering tasks
ENTRYPOINT ["bash", "-c", "airflow webserver & airflow scheduler"]
