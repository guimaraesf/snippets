#!/usr/bin/env bash

function get_image_name() {
    imageName=$(head -n 1 ${dockerfileName} | cut -d "#" -f 2)
}

dockerRegistry=bvs-bigdata
dockerfileName=Dockerfile
get_image_name ${dockerfileName}

docker run -d --name cadastral-api -p 3333:3333 \
       -e SPRING_PROFILES_ACTIVE=prod \
       ${dockerRegistry}/${imageName}