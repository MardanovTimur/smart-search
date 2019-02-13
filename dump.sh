#!/bin/bash
docker run -it --rm --link postgres_container_search:postgres -v $(pwd):/dumps postgres:latest pg_dump -h postgres -U postgres postgres > dump
