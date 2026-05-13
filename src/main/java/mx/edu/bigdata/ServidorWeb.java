package mx.edu.bigdata;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.format.DateTimeFormatter;

public class ServidorWeb {

    private static final int    PUERTO    = 3000;
    private static final String DASHBOARD = "dashboard.html";

    private static final String DB_URL  = "jdbc:postgresql://localhost:5432/emergencias_db";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "postgres123";

    private static final DateTimeFormatter FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws IOException {
        Path html = Paths.get(DASHBOARD);
        if (!Files.exists(html)) {
            System.err.println("[ERROR] No se encontro " + DASHBOARD);
            System.err.println("        Ejecuta el servidor desde la raiz del proyecto.");
            System.exit(1);
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(PUERTO), 0);

        server.createContext("/api/dashboard", exchange -> {
            if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
                responder(exchange, 405, "text/plain", "Metodo no permitido");
                return;
            }
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            try {
                String json = obtenerDatosDashboard();
                byte[] body = json.getBytes("UTF-8");
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
                System.out.printf("[%s] GET /api/dashboard  →  200%n", ahora());
            } catch (Exception e) {
                String err = "{\"error\":\"" + escape(e.getMessage()) + "\"}";
                responder(exchange, 500, "application/json", err);
                System.err.println("[ERROR API] " + e.getMessage());
            }
        });

        server.createContext("/", exchange -> {
            if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
                responder(exchange, 405, "text/plain", "Metodo no permitido");
                return;
            }
            try {
                byte[] body = Files.readAllBytes(html);
                exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
                System.out.printf("[%s] GET /  →  200%n", ahora());
            } catch (IOException e) {
                responder(exchange, 500, "text/plain", "Error al leer dashboard.html");
                System.err.println("[ERROR] " + e.getMessage());
            }
        });

        server.start();
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║   ServidorWeb iniciado                   ║");
        System.out.printf( "║   http://localhost:%-22d║%n", PUERTO);
        System.out.println("║   Ctrl+C para detener                    ║");
        System.out.println("╚══════════════════════════════════════════╝");
    }

    private static String obtenerDatosDashboard() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            StringBuilder sb = new StringBuilder("{");

            // Últimas 10 emergencias
            sb.append("\"emergencias\":[");
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT id, zona, tipo, prioridad, hora, fecha_registro " +
                    "FROM emergencias ORDER BY id DESC LIMIT 10")) {
                ResultSet rs = stmt.executeQuery();
                boolean first = true;
                while (rs.next()) {
                    if (!first) sb.append(",");
                    first = false;
                    sb.append("{")
                      .append("\"id\":").append(rs.getInt("id")).append(",")
                      .append("\"zona\":\"").append(escape(rs.getString("zona"))).append("\",")
                      .append("\"tipo\":\"").append(escape(rs.getString("tipo"))).append("\",")
                      .append("\"prioridad\":\"").append(escape(rs.getString("prioridad"))).append("\",")
                      .append("\"hora\":\"").append(rs.getTimestamp("hora").toLocalDateTime().format(FMT)).append("\",")
                      .append("\"fecha_registro\":\"").append(rs.getTimestamp("fecha_registro").toLocalDateTime().format(FMT)).append("\"")
                      .append("}");
                }
            }
            sb.append("],");

            // Totales generales
            sb.append("\"stats\":{");
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT COUNT(*) AS total, " +
                    "COUNT(*) FILTER (WHERE prioridad='ALTA') AS alta " +
                    "FROM emergencias")) {
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    sb.append("\"total\":").append(rs.getLong("total")).append(",")
                      .append("\"alta\":").append(rs.getLong("alta"));
                }
            }
            sb.append("},");

            // Por zona
            sb.append("\"zonas\":[");
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT zona, COUNT(*) AS total FROM emergencias " +
                    "GROUP BY zona ORDER BY total DESC")) {
                ResultSet rs = stmt.executeQuery();
                boolean first = true;
                while (rs.next()) {
                    if (!first) sb.append(",");
                    first = false;
                    sb.append("{\"zona\":\"").append(escape(rs.getString("zona")))
                      .append("\",\"total\":").append(rs.getLong("total")).append("}");
                }
            }
            sb.append("],");

            // Por tipo
            sb.append("\"tipos\":[");
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT tipo, COUNT(*) AS total FROM emergencias " +
                    "GROUP BY tipo ORDER BY total DESC")) {
                ResultSet rs = stmt.executeQuery();
                boolean first = true;
                while (rs.next()) {
                    if (!first) sb.append(",");
                    first = false;
                    sb.append("{\"tipo\":\"").append(escape(rs.getString("tipo")))
                      .append("\",\"total\":").append(rs.getLong("total")).append("}");
                }
            }
            sb.append("]");

            sb.append("}");
            return sb.toString();
        }
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static void responder(HttpExchange ex, int status, String contentType, String msg)
            throws IOException {
        byte[] body = msg.getBytes("UTF-8");
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(body);
        }
    }

    private static String ahora() {
        return java.time.LocalDateTime.now().format(
            java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
    }
}
