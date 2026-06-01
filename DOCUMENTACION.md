# Sistema de Emergencias Médicas CDMX — Documentación

## Descripción General

Este proyecto es un sistema de **streaming en tiempo real** para el monitoreo de emergencias médicas en la Ciudad de México. Permite ingresar datos de emergencias (reales o simulados), transportarlos a través de Apache Kafka, almacenarlos en una base de datos PostgreSQL y visualizarlos en un dashboard web con gráficas y mapa interactivo.

---

## Arquitectura del Sistema

```
┌──────────────────────┐     ┌───────────────────────┐
│  EmergenciaProducer  │     │     ProducerCSV       │
│  (datos simulados)   │     │  (datos_911_cdmx.csv) │
└──────────┬───────────┘     └──────────┬────────────┘
           │                            │
           │     Mensajes JSON          │
           └────────────┬───────────────┘
                        ▼
             ┌──────────────────────┐
             │    Apache Kafka      │
             │  topic: emergencias  │
             │       -medicas       │
             └──────────┬───────────┘
                        │
                        ▼
             ┌──────────────────────┐
             │  EmergenciaConsumer  │
             └──────────┬───────────┘
                        │  INSERT
                        ▼
             ┌──────────────────────┐
             │      PostgreSQL      │
             │   emergencias_db     │
             └──────────┬───────────┘
                        │  SELECT
                        ▼
             ┌──────────────────────┐
             │     ServidorWeb      │
             │   /api/dashboard     │
             └──────────┬───────────┘
                        │  HTTP JSON
                        ▼
             ┌──────────────────────┐
             │    dashboard.html    │
             │ Chart.js + Leaflet   │
             └──────────────────────┘
```

---

## Tecnologías Utilizadas y Para Qué Se Usó Cada Una

### Java 17
**¿Qué es?** Lenguaje de programación orientado a objetos, robusto y multiplataforma.

**¿Para qué se usó en este proyecto?**
Se usó para escribir **todo el backend** del sistema: los productores que generan y envían mensajes a Kafka, el consumidor que los recibe y los guarda en la base de datos, y el servidor web que expone la API REST. Java fue elegido porque el cliente oficial de Kafka (`kafka-clients`) está escrito en Java y tiene soporte nativo.

---

### Apache Kafka 2.7.0
**¿Qué es?** Plataforma de streaming distribuido que permite publicar y consumir flujos de eventos en tiempo real. Funciona como un intermediario (broker) entre quien produce datos y quien los consume.

**¿Para qué se usó en este proyecto?**
Kafka actúa como el **canal de transporte central** del sistema. Los productores (`EmergenciaProducer` y `ProducerCSV`) publican cada emergencia como un mensaje en el topic `emergencias-medicas`. El consumidor (`EmergenciaConsumer`) lee esos mensajes en tiempo real. Kafka garantiza que ningún mensaje se pierda y que puedan procesarse en orden.

Conceptos de Kafka usados en el proyecto:

| Concepto           | Cómo se usa aquí                                                          |
|--------------------|---------------------------------------------------------------------------|
| **Topic**          | `emergencias-medicas` es el canal donde se publican todas las emergencias |
| **Producer**       | `EmergenciaProducer` y `ProducerCSV` publican mensajes al topic           |
| **Consumer**       | `EmergenciaConsumer` lee y procesa los mensajes del topic                 |
| **Consumer Group** | `grupo-emergencias` agrupa al consumidor; permite escalar si se agregan más|
| **Partition Key**  | La zona de la emergencia es la clave; todas las emergencias de la misma zona van a la misma partición |
| **Offset**         | Kafka recuerda qué mensajes ya leyó el consumidor para no repetirlos      |
| **ACKS=all**       | El productor exige confirmación antes de considerar el mensaje enviado    |

---

### Apache ZooKeeper
**¿Qué es?** Servicio de coordinación distribuida. Kafka lo usa internamente para gestionar el estado del clúster.

**¿Para qué se usó en este proyecto?**
ZooKeeper se inicia antes que Kafka (ver `iniciar.sh`) y se encarga de registrar qué brokers están activos, gestionar los metadatos de los topics y coordinar a los consumidores dentro del grupo. Sin ZooKeeper, Kafka no puede arrancar.

---

### PostgreSQL
**¿Qué es?** Base de datos relacional de código abierto, potente y con soporte para consultas SQL avanzadas.

**¿Para qué se usó en este proyecto?**
PostgreSQL es el **almacenamiento persistente** del sistema. Cada vez que el consumidor recibe un mensaje de Kafka, inserta la emergencia en la tabla `emergencias` de la base de datos `emergencias_db`. Posteriormente, el servidor web consulta esta tabla para alimentar el dashboard. Sin PostgreSQL, los datos existirían solo en Kafka de forma temporal.

Estructura de la tabla usada:
```sql
emergencias (
  id              SERIAL PRIMARY KEY,   -- identificador autoincremental
  zona            VARCHAR,              -- alcaldía o zona de la ciudad
  tipo            VARCHAR,              -- tipo de incidente
  prioridad       VARCHAR,              -- ALTA / MEDIA / BAJA
  hora            VARCHAR,              -- hora del incidente
  fecha_registro  TIMESTAMP             -- momento en que se guardó en BD
)
```

---

### Maven
**¿Qué es?** Herramienta de gestión de proyectos Java que automatiza la descarga de dependencias y la compilación del código.

**¿Para qué se usó en este proyecto?**
Maven se usó para declarar las dos dependencias externas del proyecto en `pom.xml`: la liberia `kafka-clients` (para que Java pueda hablar con Kafka) y el driver `postgresql` (para que Java pueda conectarse a la base de datos). También se usa para ejecutar cada clase con el comando `mvn exec:java`.

---

### Chart.js 4.4.0
**¿Qué es?** Librería JavaScript para crear gráficas interactivas en el navegador.

**¿Para qué se usó en este proyecto?**
Se usó en `dashboard.html` para dibujar dos gráficas que se actualizan cada 5 segundos:
- **Gráfica de Barras** — muestra cuántas emergencias hubo por zona de la ciudad.
- **Gráfica de Dona** — muestra la distribución de emergencias por tipo de incidente.

---

### Leaflet.js 1.9.4
**¿Qué es?** Librería JavaScript para mapas interactivos en el navegador.

**¿Para qué se usó en este proyecto?**
Se usó para mostrar un **mapa de la Ciudad de México** con marcadores que indican en qué zonas ocurrieron emergencias. Cada zona del sistema tiene coordenadas asignadas y aparece como un pin en el mapa.

---

### HTML + CSS + JavaScript (Vanilla)
**¿Qué es?** Las tecnologías base de cualquier página web.

**¿Para qué se usó en este proyecto?**
Todo el frontend del dashboard (`dashboard.html`) está construido con HTML, CSS y JavaScript puro, sin frameworks como React o Vue. JavaScript hace llamadas `fetch()` al endpoint `/api/dashboard` cada 5 segundos para actualizar las gráficas, la tabla y el mapa con los datos más recientes.

---

## Flujo de Datos — Paso a Paso

El flujo completo sigue este camino:

```
CSV o Datos Simulados → Kafka → Consumer → PostgreSQL → API REST → Dashboard Web
```

A continuación se explica qué ocurre exactamente en cada etapa.

---

### PASO 1 — Origen de los Datos: CSV o Datos Simulados

Existen **dos formas** de introducir datos al sistema. Ambas producen el mismo resultado: un mensaje JSON publicado en Kafka.

**Opción A — Datos Simulados (`EmergenciaProducer.java`)**

El productor genera emergencias ficticias de forma aleatoria. Cada 500 milisegundos construye un objeto JSON con:
- Una zona elegida al azar (ej. `"Polanco"`)
- Un tipo de emergencia al azar (ej. `"Infarto"`)
- Una prioridad al azar (ej. `"ALTA"`)
- La hora actual del sistema

Ejemplo del mensaje generado:
```json
{
  "zona": "Polanco",
  "tipo": "Infarto",
  "prioridad": "ALTA",
  "hora": "2026-06-01 10:30:00"
}
```

**Opción B — Datos Reales (`ProducerCSV.java`)**

El productor lee línea por línea el archivo `datos_911_cdmx.csv`, que contiene registros reales de llamadas al 911 de la CDMX. Para cada línea válida:
1. Extrae el folio, la alcaldía, el tipo de incidente y la hora
2. Determina la prioridad según la clasificación de alarma del CSV:
   - `URGENCIAS MEDICAS` → **ALTA**
   - `EMERGENCIA` → **MEDIA**
   - Cualquier otro → **BAJA**
3. Construye el JSON y lo envía a Kafka

Ejemplo del mensaje generado:
```json
{
  "folio": "C5/20220110/02402",
  "zona": "IZTAPALAPA",
  "tipo": "Dolor",
  "prioridad": "ALTA",
  "hora": "2022-01-10 17:01:37"
}
```

En ambos casos, la **zona** se usa como clave de partición en Kafka, lo que significa que todas las emergencias de la misma zona siempre van a la misma partición y se procesan en orden.

---

### PASO 2 — Apache Kafka: Transporte del Mensaje

Una vez que el productor construye el mensaje JSON, lo publica en el topic `emergencias-medicas` del broker Kafka que corre en `localhost:9092`.

Lo que ocurre dentro de Kafka:
1. El broker recibe el mensaje y lo asigna a una **partición** del topic según la clave (la zona)
2. Le asigna un número de **offset** (posición única dentro de la partición)
3. Guarda el mensaje en disco de forma temporal
4. Confirma al productor que el mensaje fue recibido correctamente (`ACKS=all`)

Kafka actúa como una **cola de mensajes resistente**: aunque el consumidor esté caído en ese momento, los mensajes quedan guardados y se procesan cuando vuelva a estar activo.

---

### PASO 3 — Consumer: Lectura y Procesamiento

`EmergenciaConsumer.java` está suscrito al topic `emergencias-medicas` como parte del grupo `grupo-emergencias`. Cada segundo hace un **poll** (consulta) a Kafka para obtener los mensajes nuevos.

Por cada mensaje recibido, el consumidor hace lo siguiente en orden:

1. **Extrae los campos del JSON** — analiza el texto del mensaje y saca los valores de `zona`, `tipo`, `prioridad` y `hora`
2. **Actualiza contadores en memoria** — lleva la cuenta de cuántas emergencias hubo por zona y por tipo durante la sesión actual
3. **Imprime en consola** — muestra el registro con formato tabular para monitoreo en tiempo real
4. **Lanza alerta si es ALTA** — si la prioridad es `ALTA`, imprime una advertencia especial en consola indicando que se requiere atención inmediata
5. **Guarda en PostgreSQL** — inserta el registro en la tabla `emergencias` mediante una sentencia SQL `INSERT`
6. **Muestra resumen estadístico** — cada 5 emergencias procesadas, imprime en consola un resumen con el total, la zona con más casos y el tipo más frecuente

---

### PASO 4 — PostgreSQL: Almacenamiento Persistente

Cada emergencia procesada por el consumidor queda guardada permanentemente en la tabla `emergencias` de la base de datos `emergencias_db`.

La sentencia SQL que se ejecuta es:
```sql
INSERT INTO emergencias (zona, tipo, prioridad, hora, fecha_registro)
VALUES (?, ?, ?, ?, ?)
RETURNING id;
```

El campo `fecha_registro` guarda el momento exacto en que el consumidor procesó el mensaje (no la hora del incidente). El campo `id` se genera automáticamente y permite identificar cada registro de forma única.

PostgreSQL actúa como el **almacén histórico** del sistema: todos los datos que llegaron por Kafka quedan aquí guardados y disponibles para consultas futuras.

---

### PASO 5 — API REST: Exposición de los Datos

`ServidorWeb.java` es un servidor HTTP que corre en el puerto `3000`. Cuando el dashboard necesita datos, hace una petición HTTP al endpoint:

```
GET http://localhost:3000/api/dashboard
```

El servidor recibe esa petición y ejecuta **4 consultas SQL** en PostgreSQL:

| Consulta              | SQL ejecutado                                              | Datos que devuelve               |
|-----------------------|------------------------------------------------------------|----------------------------------|
| Últimas emergencias   | `SELECT ... ORDER BY id DESC LIMIT 200`                    | Lista de las últimas 200 emergencias |
| Estadísticas globales | `SELECT COUNT(*), COUNT(*) FILTER (WHERE prioridad='ALTA')`| Total de emergencias y alertas ALTA |
| Por zona              | `SELECT zona, COUNT(*) GROUP BY zona ORDER BY total DESC`  | Cuántas emergencias por alcaldía |
| Por tipo              | `SELECT tipo, COUNT(*) GROUP BY tipo ORDER BY total DESC`  | Cuántas emergencias por tipo     |

Con los resultados, construye un único objeto JSON y lo devuelve en la respuesta HTTP:

```json
{
  "emergencias": [ { "id": 1, "zona": "...", "tipo": "...", ... }, ... ],
  "stats": { "total": 150, "alta": 72 },
  "zonas": [ { "zona": "IZTAPALAPA", "total": 45 }, ... ],
  "tipos": [ { "tipo": "Infarto", "total": 30 }, ... ]
}
```

---

### PASO 6 — Dashboard Web: Visualización en Tiempo Real

`dashboard.html` es la interfaz visual del sistema. Cuando el usuario abre `http://localhost:3000` en su navegador:

1. El navegador descarga el archivo `dashboard.html` desde el servidor
2. JavaScript ejecuta automáticamente una función que llama a `/api/dashboard` cada **5 segundos**
3. Con los datos recibidos actualiza en pantalla:

| Elemento visual       | Datos que muestra                              |
|-----------------------|------------------------------------------------|
| Tarjeta "Total"       | Número total de emergencias registradas        |
| Tarjeta "Alertas ALTA"| Cuántas emergencias fueron de prioridad ALTA   |
| Gráfica de barras     | Emergencias agrupadas por zona de la ciudad    |
| Gráfica de dona       | Distribución de emergencias por tipo           |
| Mapa interactivo      | Marcadores sobre la CDMX según zona            |
| Tabla de registros    | Lista de las últimas emergencias con colores por prioridad |

La tabla muestra cada emergencia con una etiqueta de color:
- **Rojo** → Prioridad ALTA
- **Amarillo** → Prioridad MEDIA
- **Verde** → Prioridad BAJA

---

## Resumen Visual del Flujo Completo

```
┌─────────────────────────────────────────────────────────────────┐
│  PASO 1 — ORIGEN                                                │
│                                                                 │
│  datos_911_cdmx.csv ──►  ProducerCSV.java  ──┐                  │
│                                               ├──► JSON cada    │
│  Datos aleatorios   ──► EmergenciaProducer ──┘    500 ms        │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  PASO 2 — KAFKA                                                 │
│                                                                 │
│  Broker: localhost:9092                                         │
│  Topic:  emergencias-medicas                                    │
│  Guarda el mensaje con un offset único por partición            │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  PASO 3 — CONSUMER                                              │
│                                                                 │
│  EmergenciaConsumer.java                                        │
│  - Poll cada 1 segundo                                          │
│  - Extrae zona, tipo, prioridad, hora del JSON                  │
│  - Alerta en consola si prioridad = ALTA                        │
│  - Resumen estadístico cada 5 mensajes                          │
└─────────────────────────────────┬───────────────────────────────┘
                                  │  INSERT SQL
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  PASO 4 — POSTGRESQL                                            │
│                                                                 │
│  Base de datos: emergencias_db                                  │
│  Tabla: emergencias                                             │
│  Almacena cada emergencia de forma permanente                   │
└─────────────────────────────────┬───────────────────────────────┘
                                  │  SELECT SQL
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  PASO 5 — API REST                                              │
│                                                                 │
│  ServidorWeb.java — puerto 3000                                 │
│  GET /api/dashboard                                             │
│  Ejecuta 4 consultas SQL y devuelve un JSON unificado           │
└─────────────────────────────────┬───────────────────────────────┘
                                  │  HTTP + JSON (cada 5 s)
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  PASO 6 — DASHBOARD WEB                                         │
│                                                                 │
│  dashboard.html — http://localhost:3000                         │
│  - Gráfica de barras por zona     (Chart.js)                    │
│  - Gráfica de dona por tipo       (Chart.js)                    │
│  - Mapa con marcadores CDMX       (Leaflet.js)                  │
│  - Tabla con colores por prioridad (HTML + CSS)                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Cómo Iniciar el Sistema

### Prerequisitos

- Java 17 instalado
- Apache Kafka + ZooKeeper instalados en `/usr/local/`
- PostgreSQL corriendo con la base de datos `emergencias_db` y la tabla `emergencias` creada
- Maven instalado

### Pasospp

```bash
# 1. Iniciar ZooKeeper y Kafka
./iniciar.sh

# 2. En una terminal: iniciar el servidor web
mvn exec:java -Dexec.mainClass=mx.edu.bigdata.ServidorWeb

# 3. En otra terminal: iniciar el consumidor
mvn exec:java -Dexec.mainClass=mx.edu.bigdata.EmergenciaConsumer

# 4a. En otra terminal: productor con datos simulados
mvn exec:java -Dexec.mainClass=mx.edu.bigdata.EmergenciaProducer

# 4b. O bien, productor con datos reales del CSV
mvn exec:java -Dexec.mainClass=mx.edu.bigdata.ProducerCSV

# 5. Abrir el dashboard en el navegador
http://localhost:3000
```

---

## Estructura de Archivos

```
emergencias-kafka/
├── pom.xml                          # Dependencias Maven (Kafka, PostgreSQL)
├── iniciar.sh                       # Script para arrancar ZooKeeper y Kafka
├── datos_911_cdmx.csv               # Dataset real del 911 CDMX (~71 MB)
├── dashboard.html                   # Frontend SPA del dashboard
└── src/main/java/mx/edu/bigdata/
    ├── EmergenciaProducer.java      # Productor de datos simulados
    ├── ProducerCSV.java             # Productor de datos reales (CSV)
    ├── EmergenciaConsumer.java      # Consumidor + persistencia en PostgreSQL
    └── ServidorWeb.java             # Servidor HTTP + API REST
```
