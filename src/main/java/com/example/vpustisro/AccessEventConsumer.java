// src/main/java/com/example/vpustisro/AccessEventConsumer.java
package com.example.vpustisro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
//import org.springframework.kafka.annotation.KafkaListener;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

@Component
public class AccessEventConsumer {
    ObjectMapper objectMapper = new ObjectMapper();
    private final Connection connection;

    public AccessEventConsumer(DataSource dataSource) throws SQLException {
        this.connection = dataSource.getConnection();

    }

    @KafkaListener(topics = "AccessEvent")
    public void listenAccesEvents(String message) throws SQLException {
        System.out.println("Received Message: " + message);
        processEvent(message);
    }

    private void processEvent(String eventJson) throws SQLException {
        try {
            JsonNode event = objectMapper.readTree(eventJson);
            String cardId = event.get("cardId").asText();
            String locationId = event.get("locationId").asText();
            String timestamp = event.get("timestamp").asText();

            if (!isCardValid(connection, cardId)) {
                System.out.println("Access denied - invalid card: " + cardId);
                return;
            }
            if (!isLocationValid(connection, locationId)) {
                System.out.println("Access denied - invalid location: " + locationId);
                return;
            }

            String direction = determineDirection(connection, cardId);
//            System.out.println("&&&&&&&&&&&&Determined direction: " + direction);

            insertAccessLog(connection, cardId, locationId, timestamp, direction);

            System.out.println("Access granted: " + direction + " at location: " + locationId);

        } catch (Exception e) {
            System.err.println("Error processing event: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private boolean isLocationValid(Connection connection, String locationId) {
        try {
            int locationIdInt = Integer.parseInt(locationId);
            try (PreparedStatement stmt = connection.prepareStatement(
                    "SELECT 1 FROM Location WHERE location_id = ?")) {
                stmt.setInt(1, locationIdInt);
                return stmt.executeQuery().next();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isCardValid(Connection connection, String cardId) throws SQLException {
        try {
            int cardIdInt = Integer.parseInt(cardId);
            try (PreparedStatement stmt = connection.prepareStatement(
                    "SELECT 1 FROM Card WHERE card_id = ?")) {
                stmt.setInt(1, cardIdInt);
                return stmt.executeQuery().next();
            }
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private String determineDirection(Connection connection, String cardId) throws SQLException {
        String sql = """
                SELECT direction
                FROM AccessLog
                WHERE card_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, Integer.parseInt(cardId));
            ResultSet rs = stmt.executeQuery();

            if (!rs.next()) {
                // No previous entries, default to IN
//            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//            System.out.println("No previous entries found for card_id: " + cardId);
//            System.out.println("Setting direction to: IN");
                return "IN";
            }

            String lastDirection = rs.getString("direction");
//        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//        System.out.println("Last direction found for card_id " + cardId + ": " + lastDirection);

            // Toggle direction
            String newDirection = "IN".equals(lastDirection) ? "OUT" : "IN";
//        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//        System.out.println("Setting new direction to: " + newDirection);
            return newDirection;
        }
    }

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");


    private void insertAccessLog(Connection connection, String cardId, String locationId, String timestamp, String direction) throws SQLException {
        LocalDateTime dateTime = LocalDateTime.parse(timestamp, formatter);
        System.out.println("Inserting access log: " + cardId + " " + locationId + " " + timestamp + " " + direction);
        boolean originalAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false); // Disable autoCommit
            try (PreparedStatement stmt = connection.prepareStatement(
                    "INSERT INTO AccessLog (card_id, location_id, timestamp, direction) VALUES (?, ?, ?, ?)")) {
                stmt.setInt(1, Integer.parseInt(cardId));
                stmt.setInt(2, Integer.parseInt(locationId));
                stmt.setTimestamp(3, Timestamp.valueOf(dateTime));
                stmt.setString(4, direction);
//                System.out.println("##########Executing SQL with direction: " + direction);
                stmt.executeUpdate();
                connection.commit(); // Commit the transaction
            } catch (SQLException e) {
                connection.rollback(); // Rollback in case of an error
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit); // Restore original autoCommit setting
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error setting autoCommit", e);
        }
    }
}


//    public AccessEventConsumer(DataSource dataSource) throws SQLException {
//        this.dataSource = dataSource;
//        this.connection = dataSource.getConnection();
//
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("group.id", "access-log-group");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "latest");  // Change to 'latest' to avoid reprocessing
//        props.put("enable.auto.commit", "true");   // Enable auto commit of offsets
//        props.put("auto.commit.interval.ms", "1000"); // Commit offsets every second
//
//        consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Collections.singletonList("AccessEvent"));
//        objectMapper = new ObjectMapper();
//    }

//    @EventListener(ApplicationReadyEvent.class)
//    public void startConsumer() {
//        new Thread(this::consumeAndProcessEvents).start();
//    }

//    public void consumeAndProcessEvents() {
//        try {
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//
//                if (!records.isEmpty()) {
//                    records.forEach(record -> {
//                        try {
//                            // Re-establish connection for each batch to prevent stale connections
//                            try (Connection connection = dataSource.getConnection()) {
//                                processEvent(connection, record.value());
//                            }
//                        } catch (Exception e) {
//                            System.err.println("Error processing record: " + e.getMessage());
//                            e.printStackTrace();
//                        }
//                    });
//                }
//            }
//        } catch (Exception e) {
//            System.err.println("Error while consuming events: " + e.getMessage());
//            e.printStackTrace();
//        } finally {
//            consumer.close();
//        }
//    }