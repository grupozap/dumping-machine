FROM openjdk:11-jre

WORKDIR /opt/app/

ADD build/install/dumping-machine/ /opt/app/

CMD ./bin/dumping-machine
