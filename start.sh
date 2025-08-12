#!/bin/bash
set -e

docker-compose up -d
sleep 10

# init mongodb replica set
docker exec -it mongodb_primary ./init-replica.sh
