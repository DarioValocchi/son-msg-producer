FROM maven:3.5.0-jdk-8
MAINTAINER D.Valocchi <d.valocchi@ucl.ac.uk>

RUN apt-get update && apt-get upgrade -y

ADD ./mvn-project /msg-producer

WORKDIR /msg-producer

RUN mvn -e compile assembly:single;


#ENTRYPOINT ls target
#ENTRYPOINT java
ENTRYPOINT ["java", "-jar", "target/RabbitMqProducer-0.0.1-SNAPSHOT-jar-with-dependencies.jar"]
#ENTRYPOINT mvn exec:java -Dexec.mainClass="org.sonata.main.ProducerCLI"

CMD ["--folder", "./messages", "--url", "amqp://guest:guest@son-broker:5672/%2F", "--threads", "5", "--requests", "5"]
