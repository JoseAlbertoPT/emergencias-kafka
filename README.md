# Sistema de Monitoreo de Emergencias Médicas en Tiempo Real
## Apache Kafka + Java + PostgreSQL | Proyecto Big Data

Sistema que simula una central de emergencias médicas usando arquitectura Producer-Consumer con Apache Kafka, guardando datos en PostgreSQL y mostrando un dashboard web.

---

## Tecnologías
- Java 17
- Apache Kafka 2.7.0
- Apache Zookeeper 3.7.0
- PostgreSQL
- Maven

---

## Pasos para ejecutar el proyecto

### Terminal 1 — Iniciar Zookeeper
```bash
sudo zk-start
```

### Terminal 2 — Iniciar Kafka
```bash
sudo kafka-start
```

### Terminal 3 — Iniciar Consumer
```bash
cd ~/emergencias-kafka
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mvn exec:java -Dexec.mainClass="mx.edu.bigdata.EmergenciaConsumer"
```

### Terminal 4 — Iniciar Producer
```bash
cd ~/emergencias-kafka
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mvn exec:java -Dexec.mainClass="mx.edu.bigdata.EmergenciaProducer"
```
## Terminal 4B — Producer con datos reales 911 CDMX
```bash
cd ~/emergencias-kafka
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mvn exec:java -Dexec.mainClass="mx.edu.bigdata.ProducerCSV"
```
Usa el archivo datos_911_cdmx.csv con 390,063 registros reales 

### Terminal 5 — Servidor Web Dashboard
```bash
cd ~/emergencias-kafka
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mvn exec:java -Dexec.mainClass="mx.edu.bigdata.ServidorWeb"
```

Abrir navegador en: http://localhost:3000/dashboard.html

---

## Arquitectura del sistema
**Flujo de datos:**
1. El Producer genera una emergencia aleatoria en formato JSON
2. La publica en el topic de Kafka cada 3 segundos
3. Kafka almacena y distribuye el mensaje
4. El Consumer recibe el mensaje, lo analiza y lo guarda en PostgreSQL
5. Cada 5 mensajes muestra estadísticas en consola
6. El dashboard web muestra los datos en tiempo real

**Ejemplo de mensaje JSON:**
```json
{
  "zona": "Reforma",
  "tipo": "Infarto",
  "prioridad": "ALTA",
  "hora": "13:25:01"
}
```

---

## Consultas SQL útiles

```sql
SELECT * FROM emergencias ORDER BY fecha_registro DESC LIMIT 10;
SELECT zona, COUNT(*) as total FROM emergencias GROUP BY zona ORDER BY total DESC;
SELECT tipo, COUNT(*) as total FROM emergencias GROUP BY tipo ORDER BY total DESC;
SELECT * FROM emergencias WHERE prioridad = 'ALTA' ORDER BY fecha_registro DESC;
```
