FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2-binary tqdm

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "sh", "-c", "python ingest_data.py" ]
CMD ["--url=${URL} --user=${USER} --password=${PASSWORD} --host=${HOST} --port=${PORT} --db_name=${DB_NAME} --table_name=${TABLE_NAME}"]