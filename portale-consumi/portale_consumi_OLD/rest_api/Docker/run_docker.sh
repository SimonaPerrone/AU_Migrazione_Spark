#!/bin/bash

PORT=80

docker run --name api_rest -d --restart=always -p $PORT:8086 rest_api_image &&\
docker logs -f api_rest