FROM openjdk:11-jre

WORKDIR /opt/app/

ADD build/libs/dumping-machine*.jar dumping-machine.jar

CMD java $JAVA_OPTS -jar dumping-machine.jar