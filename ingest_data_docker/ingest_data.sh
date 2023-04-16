

# -v "$(pwd):/app/mount"
docker run --rm -it \
    --network my_app_network \
      ingest_data:v001 \
    --url=$1 \
    --user=user --password=password --host=postgres_db_zoomcamp --port="5432" --db_name=ny_taxi --table_name=test
