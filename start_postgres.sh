#!/bin/bash
docker run -it --name postgres_container_search --user 1000:1000 -e POSTGRES_PASSWORD=postgres --publish 4321:5432 -v crauler_search:/var/lib/postgresql/data -d postgres
