#!/usr/bin/env bash
docker run --rm -d -p 27017:27017 --name mongo -h $(hostname) mongo:4.2.5 --replSet=test
sleep 3
docker exec mongo mongo --eval "rs.initiate();"

