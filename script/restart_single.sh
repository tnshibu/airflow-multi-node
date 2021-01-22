#!/bin/bash
docker-compose -f ./docker-compose-LocalExecutor.yml down
docker-compose -f ./docker-compose-LocalExecutor.yml up -d