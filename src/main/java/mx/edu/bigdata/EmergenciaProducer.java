package mx.edu.bigdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

public class EmergenciaProducer {

    private static final String TOPIC = "emergencias-medicas";
    private static final String BROKER = "localhost:9092";

    private static final String[] ZONAS = {
        "Reforma", "Polanco", "Condesa", "Centro Historico",
        "Coyoacan", "Santa Fe", "Tlalpan", "Iztapalapa"
    };

    private static final String[] TIPOS = {
        "Infarto", "Accidente Vial", "Trauma Severo",
        "Paro Cardiorrespiratorio", "Quemaduras", "Intoxicacion"
    };


    private static final String[] PRIORIDADES = {"ALTA", "ALTA", "MEDIA", "BAJA"};
    private static final DateTimeFormatter FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        Random random = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println(" EmergenciaProducer iniciado. Enviando al topic: " + TOPIC + " ===");

            while (true) {
                String zona      = ZONAS[random.nextInt(ZONAS.length)];
                String tipo      = TIPOS[random.nextInt(TIPOS.length)];
                String prioridad = PRIORIDADES[random.nextInt(PRIORIDADES.length)];
                String hora      = LocalDateTime.now().format(FORMATTER);

                String json = String.format(
                    "{\"zona\":\"%s\",\"tipo\":\"%s\",\"prioridad\":\"%s\",\"hora\":\"%s\"}",
                    zona, tipo, prioridad, hora
                );

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, zona, json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error al enviar: " + exception.getMessage());
                    } else {
                        System.out.printf("[ENVIADO] Particion=%d  Offset=%d  %s%n",
                            metadata.partition(), metadata.offset(), json);
                    }
                });

                Thread.sleep(3000);
            }
        }
    }
}
