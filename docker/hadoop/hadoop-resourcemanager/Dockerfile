FROM hadoop-base:latest
LABEL maintainer="seilylook"

COPY start-resourcemanager.sh start-resourcemanager.sh

RUN chmod +x start-resourcemanager.sh

EXPOSE 8088

CMD [ "./start-resourcemanager.sh" ]