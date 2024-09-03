#!/bin/bash

echo Loading for $1

curl http://localhost:8080/v1/loadtest/$1

# while true; do 
#     curl http://localhost:8080/v1/status
#     sleep 10
# done