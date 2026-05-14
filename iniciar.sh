#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

echo "Iniciando Zookeeper..."
sudo -E /usr/local/zookeeper/bin/zkServer.sh start
sleep 3

echo "Iniciando Kafka..."
sudo -E /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &
sleep 5

echo "Sistema listo!"
echo "Ahora abre 3 terminales mas y ejecuta:"
echo "Consumer: cd /home/albert/emergencias-kafka && mvn exec:java -Dexec.mainClass=mx.edu.bigdata.EmergenciaConsumer"
echo "Producer: cd /home/albert/emergencias-kafka && mvn exec:java -Dexec.mainClass=mx.edu.bigdata.EmergenciaProducer"
echo "Servidor: cd /home/albert/emergencias-kafka && mvn exec:java -Dexec.mainClass=mx.edu.bigdata.ServidorWeb"
