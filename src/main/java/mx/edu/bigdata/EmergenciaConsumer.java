package mx.edu.bigdata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class EmergenciaConsumer {

    private static final String TOPIC   = "emergencias-medicas";
    private static final String BROKER  = "localhost:9092";
    private static final String GROUP   = "grupo-emergencias";

    private static final String DB_URL  = "jdbc:postgresql://localhost:5432/emergencias_db";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "postgres123";

    private static final String SQL_INSERT =
        "INSERT INTO emergencias (zona, tipo, prioridad, hora, fecha_registro) " +
        "VALUES (?, ?, ?, ?, ?) RETURNING id";

    private static final DateTimeFormatter HORA_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static int totalEmergencias = 0;
    private static final Map<String, Integer> conteoZonas = new HashMap<>();
    private static final Map<String, Integer> conteoTipos = new HashMap<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println("=== EmergenciaConsumer iniciado. Escuchando topic: " + TOPIC + " ===");
            System.out.println("=== Conexion a PostgreSQL establecida ===\n");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    procesarMensaje(record.value(), conn);
                }
            }

        } catch (SQLException e) {
            System.err.println("[ERROR] No se pudo conectar a PostgreSQL: " + e.getMessage());
            System.err.println("Verifique que la BD 'emergencias_db' exista y la tabla 'emergencias' este creada.");
        }
    }

    private static void procesarMensaje(String json, Connection conn) {
        String zona      = extraerCampo(json, "zona");
        String tipo      = extraerCampo(json, "tipo");
        String prioridad = extraerCampo(json, "prioridad");
        String hora      = extraerCampo(json, "hora");

        totalEmergencias++;
        conteoZonas.merge(zona, 1, Integer::sum);
        conteoTipos.merge(tipo, 1, Integer::sum);

        System.out.printf("[%s] Zona: %-18s | Tipo: %-26s | Prioridad: %s%n",
            hora, zona, tipo, prioridad);

        if ("ALTA".equals(prioridad)) {
            System.out.println("   ALERTA PRIORIDAD ALTA *** Requiere atencion inmediata en " + zona);
        }

        guardarEnBD(conn, zona, tipo, prioridad, hora);

        if (totalEmergencias % 5 == 0) {
            mostrarResumen();
        }
    }

    private static void guardarEnBD(Connection conn, String zona, String tipo,
                                    String prioridad, String horaStr) {
        try (PreparedStatement stmt = conn.prepareStatement(SQL_INSERT)) {
            LocalDateTime horaEmergencia = LocalDateTime.parse(horaStr, HORA_FORMATTER);

            stmt.setString(1, zona);
            stmt.setString(2, tipo);
            stmt.setString(3, prioridad);
            stmt.setTimestamp(4, Timestamp.valueOf(horaEmergencia));
            stmt.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    System.out.println("  [BD] Emergencia guardada con ID: " + rs.getInt("id"));
                }
            }
        } catch (SQLException e) {
            System.err.println("  [BD ERROR] No se pudo guardar: " + e.getMessage());
        }
    }

    private static void mostrarResumen() {
        String zonaTop = conteoZonas.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("N/A");

        String tipoTop = conteoTipos.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("N/A");

        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║           RESUMEN ESTADISTICO                    ║");
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.printf( "║  Total de emergencias   : %-22d║%n", totalEmergencias);
        System.out.printf( "║  Zona con mas casos     : %-22s║%n", zonaTop + " (" + conteoZonas.get(zonaTop) + ")");
        System.out.printf( "║  Tipo mas frecuente     : %-22s║%n", tipoTop + " (" + conteoTipos.get(tipoTop) + ")");
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.println("║  Emergencias por zona:                           ║");
        conteoZonas.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .forEach(e -> System.out.printf("║    %-20s : %-24d║%n", e.getKey(), e.getValue()));
        System.out.println("╚══════════════════════════════════════════════════╝\n");
    }

    private static String extraerCampo(String json, String campo) {
        String clave = "\"" + campo + "\":\"";
        int inicio = json.indexOf(clave);
        if (inicio == -1) return "";
        inicio += clave.length();
        int fin = json.indexOf("\"", inicio);
        return fin == -1 ? "" : json.substring(inicio, fin);
    }
}
