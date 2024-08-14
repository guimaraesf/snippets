FROM adoptopenjdk/openjdk8:jdk8u362-b09-alpine-slim

ENV versaoApi="0"
RUN ls
COPY core-api/src/main/resources/*.yml /api-base-cadastral/
COPY core-api/src/main/resources/oauth2-redirect.html /api-base-cadastral/
COPY core-api/src/main/resources/logback.xml /api-base-cadastral/
COPY core-api/pom.xml /api-base-cadastral/
COPY target/core-api-*.jar /api-base-cadastral/

WORKDIR /api-base-cadastral
#RUN echo `ls core-api*dependencies.jar | cut -d'-' -f3` > /versao.txt
#RUN cat /versao.txt
RUN ls
RUN rm *shaded.jar
RUN mv core-api-*.jar cadastral-api.jar
RUN ls
#RUN echo "#!/bin/bash" > /start.sh
#RUN echo "java -server -XX:+UseStringDeduplication -XX:InitiatingHeapOccupancyPercent=45 -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:ParallelGCThreads=20 -XX:ConcGCThreads=5 -Dlogging.config=file:/api-base-cadastral/logback.xml -Dspring.config.location=file:/api-base-cadastral/application.yml,/api-base-cadastral/application-dePara.yml,/api-base-cadastral/application-socket.yml -jar /api-base-cadastral/core-api-`cat /versao.txt`" >> start.sh
#RUN chmod +x /start.sh

#ENTRYPOINT ["/bin/sh /start.sh"]
#ENTRYPOINT [ "/bin/bash", "-c", "source ~/.bashrc && ./start.sh ${@}", "--" ]
ENTRYPOINT ["java", "-server", "-XX:+UseStringDeduplication", "-XX:InitiatingHeapOccupancyPercent=45", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200", "-XX:ParallelGCThreads=20", "-XX:ConcGCThreads=5", "-Dlogging.config=file:/api-base-cadastral/logback.xml", "-Dspring.config.location=file:/api-base-cadastral/application.yml,/api-base-cadastral/application-dePara.yml,/api-base-cadastral/application-socket.yml", "-jar", "cadastral-api.jar"]
