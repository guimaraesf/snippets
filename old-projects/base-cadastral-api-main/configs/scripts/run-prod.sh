#!/usr/bin/env bash
set -e

INTROSCOPE_AGENT_CONFIG=""
if [ -n "$INTROSCOPE_AGENT_HOME" ]; then
    INTROSCOPE_AGENT_CONFIG="-javaagent:${INTROSCOPE_AGENT_HOME}/Agent.jar -DagentProfile=${INTROSCOPE_AGENT_HOME}/core/config/IntroscopeAgent.profile"
else
    echo "Agente IntroScope NÃO CONFIGURADO !"
fi

java_path="/opt/java/jdk1.8.0_162/bin/"
${java_path}java -version
nohup ${java_path}java -jar \
        -server -Xms12G -Xmx12G \
        -XX:+UseStringDeduplication -XX:+UnlockExperimentalVMOptions \
        -XX:+UseG1GC -XX:ParallelGCThreads=8 -XX:MaxGCPauseMillis=100 -XX:G1NewSizePercent=6 \
        ${INTROSCOPE_AGENT_CONFIG} \
        -Dspring.profiles.active=prod,exadata \
        -Dlogging.config=file:${CADASTRAL_API_LOCAL_HOME}/log4j2.custom.xml \
        -Dspring.config.location=file:${CADASTRAL_API_LOCAL_HOME}/application.yml,${CADASTRAL_API_LOCAL_HOME}/application-dePara.yml,${CADASTRAL_API_LOCAL_HOME}/application-socket.yml \
        ${CADASTRAL_API_LOCAL_HOME}/core-api-${CADASTRAL_API_VERSION}.jar >/dev/null 2>&1 &
