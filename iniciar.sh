#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

echo "======================================"
echo "  SISTEMA DE EMERGENCIAS MEDICAS"
echo "  Iniciando todos los servicios..."
echo "======================================"

# Iniciar Zookeeper
echo "[1/2] Iniciando Zookeeper..."
sudo /usr/local/zookeeper/bin/zkServer.sh start
sleep 3

# Iniciar Kafka
echo "[2/2] Iniciando Kafka..."
sudo -E bash -c "export JAVA_HOME=$JAVA_HOME && /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &"
sleep 5

echo "======================================"
echo "  Todo listo! Ahora ejecuta:"
echo "  Terminal 3: mvn exec:java -Dexec.mainClass=mx.edu.bigdata.EmergenciaConsumer"
echo "  Terminal 4: mvn exec:java -Dexec.mainClass=mx.edu.bigdata.EmergenciaProducer"
echo "======================================"
