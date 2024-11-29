package com.example.vpustisro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;


@Component
public class TimeRequestConsumer {
    private final Connection connection;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    //    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TimeRequestProducer timeRequestProducer;
    ObjectMapper objectMapper = new ObjectMapper();


    public TimeRequestConsumer(DataSource dataSource, TimeRequestProducer timeRequestProducer) throws SQLException {
        this.connection = dataSource.getConnection();
        this.timeRequestProducer = timeRequestProducer;
    }

    @KafkaListener(topics = "TimeCalculationRequests")
    public void listenTimeRequestEvents(String message) {
        System.out.println("Received Message: " + message);
        processRequestEvent(message);
    }


    private void processRequestEvent(String eventJson) {
        try {
            JsonNode event = objectMapper.readTree(eventJson);
            String cardId = event.get("cardId").asText();
            String timestamp = event.get("timestamp").asText();

            double timeSpentInsideSeconds = calculateTimeSpentInside(cardId, timestamp);
            long hours = (long) (timeSpentInsideSeconds / 3600);
            long minutes = (long) ((timeSpentInsideSeconds % 3600) / 60);

            System.out.println("Time spent inside: " + hours + " hours and " + minutes + " minutes for card: " + cardId);
//            sendTimeCalculationResponse(cardId, hours, minutes);
            timeRequestProducer.sendTimeCalculationResponse(cardId, hours, minutes);

        } catch (Exception e) {
            System.err.println("Error processing event: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private int calculateTimeSpentInside(String cardId, String timestamp) {
        List<AccessLog> accessLogs = getAccessLogForDay(cardId, timestamp);
        if (accessLogs.isEmpty()) {
            System.out.println("No access logs found for card: " + cardId);
            return 0;
        }

        int result = 0;

        LocalDateTime previousTimeStamp = LocalDateTime.parse(timestamp, formatter).withSecond(0).withMinute(0).withHour(0);
        for (int i = 0; i < accessLogs.size(); i++) {
            AccessLog currentLog = accessLogs.get(i);
            if (!currentLog.directionIsInside) {
                result += (int) Duration.between(previousTimeStamp, currentLog.timestamp.toLocalDateTime()).getSeconds();
                System.out.println(result);
            }
            previousTimeStamp = currentLog.timestamp.toLocalDateTime();
        }
        if (accessLogs.getLast().directionIsInside) {
            result += (int) Duration.between(previousTimeStamp, LocalDateTime.parse(timestamp, formatter)).getSeconds();
        }
        return result;

    }


    private List<AccessLog> getAccessLogForDay(String cardId, String timestamp) {
        try {
            int cardIdInt = Integer.parseInt(cardId);
            LocalDateTime dateTime = LocalDateTime.parse(timestamp, formatter);
            LocalDateTime startOfTheDay = dateTime.withSecond(0).withMinute(0).withHour(0);
            try (PreparedStatement stmt = connection.prepareStatement(
                    "SELECT * FROM accesslog WHERE card_id = ? AND timestamp > ? AND timestamp < ? ORDER BY timestamp")) {
                stmt.setInt(1, cardIdInt);
                stmt.setTimestamp(2, Timestamp.valueOf(startOfTheDay));
                stmt.setTimestamp(3, Timestamp.valueOf(dateTime));
                ResultSet resultSet = stmt.executeQuery();
                List<AccessLog> accessLogs = new LinkedList<>();
                while (resultSet.next()) {
                    System.out.println("........................");
                    System.out.println(resultSet.getString("direction"));
                    System.out.println(resultSet.getTimestamp("timestamp"));
                    accessLogs.add(new AccessLog(resultSet.getString("direction"), resultSet.getTimestamp("timestamp")));
                }
                return accessLogs;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }

    class AccessLog {
        boolean directionIsInside;
        Timestamp timestamp;

        public AccessLog(String directionIsInside, Timestamp timestamp) {
            this.directionIsInside = directionIsInside.equals("IN");
            this.timestamp = timestamp;
        }
    }


}
