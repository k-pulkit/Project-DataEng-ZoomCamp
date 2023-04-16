
network_name=my_app_network
if docker network ls | awk -F ' ' '{print $2}' | grep $network_name; then
    echo "network exists"
else 
    echo "creating required network"
    docker network create $network_name
fi

# Run the postgres database, and add it to application network for access 

docker run --rm -itd \
    -v "$(pwd)/ny_taxi_postgres_data":/var/lib/postgresql/data \
    -p 5431:5432 \
    --network my_app_network \
    --name postgres_db_zoomcamp \
        postgres:v1