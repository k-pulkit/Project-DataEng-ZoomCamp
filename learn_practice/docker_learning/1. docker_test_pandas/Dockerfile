FROM python:3.9

WORKDIR /app
COPY test_pipeline.py pipeline.py
COPY test_notebook.ipynb test_notebook.ipynb

RUN pip install pandas jupyter sqlalchemy psycopg2-binary

EXPOSE 8888

ENTRYPOINT [ "bash" ]