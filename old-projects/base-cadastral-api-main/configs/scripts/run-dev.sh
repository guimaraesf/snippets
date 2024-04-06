#!/usr/bin/env bash
set -e

java -version
nohup java -jar \
	-Dcom.sun.management.jmxremote \
    -Dcom.sun.management.jmxremote.port=9098 \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.local.only=false \
    -Djava.rmi.server.hostname=localhost \
	-server \
    -XX:+UseStringDeduplication -XX:InitiatingHeapOccupancyPercent=70 \
    -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:ParallelGCThreads=20 -XX:ConcGCThreads=5 \
	-Dspring.profiles.active=dev \
	-Dlogging.config=file:${CADASTRAL_API_LOCAL_HOME}/log4j2.custom.xml \
    -Dspring.config.location=file:${CADASTRAL_API_LOCAL_HOME}/application.yml,${CADASTRAL_API_LOCAL_HOME}/application-dePara.yml,${CADASTRAL_API_LOCAL_HOME}/application-socket.yml \
	${CADASTRAL_API_LOCAL_HOME}/core-api-${CADASTRAL_API_VERSION}.jar >/dev/null 2>&1 &