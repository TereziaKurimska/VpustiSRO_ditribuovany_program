package com.example.vpustisro;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

@Component
public class TimeRequestProducer {
    public final KafkaTemplate<String, String> timeProducer;
    private final String topic;
    private final Random random = new Random();
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");


    private TimeRequestProducer(KafkaTemplate<String, String> timeProducer){
        this.timeProducer = timeProducer;
        this.topic = "TimeCalculationRequests";
    }

    private LocalDateTime startDateTime = LocalDateTime.of(2024, 1, 1, 6, 0, 0);
    private Integer order = 0;

    @Scheduled(fixedDelay = 5000)
    public void sendAccessEvent() {
        order+=2;

        String cardId = String.valueOf(random.nextInt(3) + 1);
        String timestamp =  startDateTime.plusHours(order).format(formatter); // Current timestamp

        String messageValue = String.format("{\"cardId\":\"%s\", \"timestamp\":\"%s\"}", cardId, timestamp);
        timeProducer.send(topic,cardId,messageValue);
        System.out.printf("Message sent to topic %s: %s%n", topic, messageValue);
    }

    public void sendTimeCalculationResponse(String cardId, long hours, long minutes) {
        String responseMessage = String.format("{\"cardId\":\"%s\", \"hours\":%d, \"minutes\":%d}", cardId, hours, minutes);
        timeProducer.send("TimeCalculationResponse", cardId, responseMessage);
        System.out.println(".....Sent response message: " + responseMessage);
    }
}
