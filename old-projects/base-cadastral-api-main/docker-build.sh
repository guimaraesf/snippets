#!/usr/bin/env bash

function get_image_name() {
    imageName=$(head -n 1 ${dockerfileName} | cut -d "#" -f 2)
}

dockerRegistry=bvs-bigdata
dockerfileName=Dockerfile.build
get_image_name ${dockerfileName}

# Build docker maven image
docker build -f ${dockerfileName} \
             -t ${dockerRegistry}/${imageName} \
             --cache-from ${dockerRegistry}/${imageName} .


# Build app using maven image builded
docker run -it --rm --name build-cadastral-api \
    -v `pwd`:/usr/src/app \
    -v "$HOME"/.m2:/root/.m2 \
    ${dockerRegistry}/${imageName} clean package


# Build docker api image
dockerfileName=Dockerfile
get_image_name ${dockerfileName}

docker build --no-cache -t ${imageName} -f ${dockerfileName} \
     --build-arg EXPOSE_PORT=3333 \
     --build-arg JAR_FILE=rest-api-v1/target/rest-api-v1-1.1.jar .

docker tag ${imageName} ${dockerRegistry}/${imageName}

#docker login --username ${dockerRegistryUser} --password ${dockerRegistryPass} ${dockerRegistry}
#docker push ${dockerRegistry}/${imageName}