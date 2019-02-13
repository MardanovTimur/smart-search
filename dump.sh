#!/bin/bash
docker run -it --rm --link cocky_borg:postgres -v $(pwd):/dumps postgres:latest pg_dump -h postgres -U postgres postgres > dump
