package mx.edu.bigdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ProducerCSV {

    private static final String TOPIC = "emergencias-medicas";
    private static final String BROKER = "localhost:9092";
    private static final String CSV_PATH = "datos_911_cdmx.csv";
    private static final int IDX_FOLIO            = 0;
    private static final int IDX_INCIDENTE        = 2;
    private static final int IDX_FECHA_CREACION   = 5;
    private static final int IDX_HORA_CREACION    = 6;
    private static final int IDX_CLAS_ALARMA      = 12;
    private static final int IDX_ALCALDIA         = 13;
    private static final int MIN_COLUMNS          = 18;

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = new BufferedReader(new FileReader(CSV_PATH))) {
            System.out.println("=== ProducerCSV iniciado. Enviando al topic: " + TOPIC + " ===");
            String line;
            boolean primeraFila = true;
            int enviados = 0;
            while ((line = reader.readLine()) != null) {
                if (primeraFila) {
                    primeraFila = false;
                    continue;
                }
                try {
                    String[] cols = line.split(",", -1);
                    if (cols.length < MIN_COLUMNS) {
                        System.err.printf("[SKIP] Columnas insuficientes (%d): %s%n", cols.length, line);
                        continue;
                    }
                    String folio    = cols[IDX_FOLIO].trim();
                    String tipo     = cols[IDX_INCIDENTE].trim();
                    String hora     = cols[IDX_FECHA_CREACION].trim() + " " + cols[IDX_HORA_CREACION].trim();
                    String alarma   = cols[IDX_CLAS_ALARMA].trim();
                    String zona     = cols[IDX_ALCALDIA].trim();
                    String prioridad;
                    if (alarma.contains("URGENCIAS MEDICAS")) {
                        prioridad = "ALTA";
                    } else if (alarma.contains("EMERGENCIA")) {
                        prioridad = "MEDIA";
                    } else {
                        prioridad = "BAJA";
                    }
                    final String json = String.format(
                        "{\"folio\":\"%s\",\"zona\":\"%s\",\"tipo\":\"%s\",\"prioridad\":\"%s\",\"hora\":\"%s\"}",
                        esc(folio), esc(zona), esc(tipo), prioridad, esc(hora)
                    );

                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, zona, json);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("[ERROR] Al enviar: " + exception.getMessage());
                        } else {
                            System.out.printf("[ENVIADO] Particion=%d  Offset=%d  %s%n",
                                metadata.partition(), metadata.offset(), json);
                        }
                    });
                    enviados++;
                    Thread.sleep(500);

                } catch (Exception e) {
                    System.err.println("[ERROR] Fila omitida: " + e.getMessage());
                }
            }

            producer.flush();
            System.out.printf("=== ProducerCSV finalizado. Registros enviados: %d ===%n", enviados);

        } catch (IOException e) {
            System.err.println("[FATAL] No se pudo leer el archivo CSV: " + e.getMessage());
        }
    }

    private static String esc(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
