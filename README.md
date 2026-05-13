# Sistema de Monitoreo de Emergencias Médicas en Tiempo Real
## Apache Kafka + Java | Proyecto Big Data

Sistema que simula una central de emergencias médicas usando arquitectura Producer-Consumer con Apache Kafka.

---

## Tecnologías
- Java 17
- Apache Kafka 2.7.0
- Apache Zookeeper 3.7.0
- Maven

---

## Requisitos previos
- Ubuntu Linux
- Java 17 instalado en /usr/lib/jvm/java-17-openjdk-amd64
- Apache Zookeeper instalado en /usr/local/zookeeper
- Apache Kafka instalado en /usr/local/kafka

---

## Pasos para ejecutar el proyecto

### Terminal 1 — Iniciar Zookeeper
```bash
sudo -i
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
cd /usr/local/zookeeper/bin
./zkServer.sh start
```

### Terminal 2 — Iniciar Kafka
```bash
sudo -i
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
cd /usr/local/kafka/bin
./kafka-server-start.sh ../config/server.properties
```

### Terminal 3 — Iniciar Consumer
```bash
cd /home/albert/emergencias-kafka
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mvn exec:java -Dexec.mainClass="mx.edu.bigdata.EmergenciaConsumer"
```

### Terminal 4 — Iniciar Producer
```bash
cd /home/albert/emergencias-kafka
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mvn exec:java -Dexec.mainClass="mx.edu.bigdata.EmergenciaProducer"
```

---

## Arquitectura del sistema
