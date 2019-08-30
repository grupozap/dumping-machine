FROM gradle:jdk8 as builder

LABEL maintainer="rubensmabueno@hotmail.com,rafael.paixao@outlook.com"

COPY --chown=gradle:gradle . /home/gradle/src/
WORKDIR /home/gradle/src/
RUN gradle clean distTar

FROM openjdk:8-jre-slim

COPY --from=builder /home/gradle/src/build/distributions/dumping-machine.tar /opt/dumping-machine.tar

WORKDIR /opt

RUN tar -xvf dumping-machine.tar && rm dumping-machine.tar

WORKDIR /opt/dumping-machine

CMD ./bin/dumping-machine