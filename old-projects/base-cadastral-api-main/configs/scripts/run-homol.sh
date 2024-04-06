#!/usr/bin/env bash
set -e

java -version
nohup java -jar \
        -server -Xms4G -Xmx4G \
        -XX:+UseStringDeduplication -XX:+UnlockExperimentalVMOptions \
        -XX:+UseG1GC -XX:ParallelGCThreads=8 -XX:MaxGCPauseMillis=100 -XX:G1NewSizePercent=6 \
        -Dspring.profiles.active=homol \
        -Dlogging.config=file:${CADASTRAL_API_LOCAL_HOME}/log4j2.custom.xml \
        -Dspring.config.location=file:${CADASTRAL_API_LOCAL_HOME}/application.yml,${CADASTRAL_API_LOCAL_HOME}/application-dePara.yml,${CADASTRAL_API_LOCAL_HOME}/application-socket.yml \
        ${CADASTRAL_API_LOCAL_HOME}/core-api-${CADASTRAL_API_VERSION}.jar >/dev/null 2>&1 &