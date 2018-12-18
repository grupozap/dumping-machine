FROM openjdk:11-jre

LABEL maintainer="rubensmabueno@hotmail.com,rafael.paixao@outlook.com"

WORKDIR /opt/app/

ADD build/install/dumping-machine/ /opt/app/

CMD ./bin/dumping-machine
