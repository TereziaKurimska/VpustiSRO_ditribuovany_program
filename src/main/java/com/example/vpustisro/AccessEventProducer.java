package com.example.vpustisro;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

@Component
public class AccessEventProducer {
    private final KafkaTemplate<String, String> producer;
    private final String topic;
    private final Random random = new Random();
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");


    private AccessEventProducer(KafkaTemplate<String, String> producer){
        this.producer = producer;
        this.topic = "AccessEvent";
    }

    private LocalDateTime startDateTime = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
    private Integer order = 0;

    @Scheduled(fixedDelay = 500)
    public void sendAccessEvent() {
        order++;
        String cardId = String.valueOf(random.nextInt(5) + 1); // Random cardId between 1 and 5
        String locationId = String.valueOf(random.nextInt(2) + 1); // Random locationId between 1 and 5
        String timestamp =  startDateTime.plusHours(order).format(formatter); // Current timestamp

        String messageValue = String.format("{\"cardId\":\"%s\", \"locationId\":\"%s\", \"timestamp\":\"%s\"}", cardId, locationId, timestamp);
        producer.send(topic, cardId, messageValue);
        System.out.printf("Message sent to topic %s: %s%n", topic, messageValue);
    }


    //    public AccessEventProducer(KafkaProducer<String, String> producer, String topic) {
//        this.producer = producer;
//        this.topic = topic;
//    }

//    public AccessEventProducer(String kafkaBroker, String topic) {
//        this.topic = topic;
//
//        Properties props = new Properties();
//        props.put("bootstrap.servers", kafkaBroker);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//        this.producer = new KafkaProducer<>(props);
//    }
//
//    public void sendAccessEvent(String cardId, String locationId, String timestamp) {
//        String messageValue = String.format("{\"cardId\":\"%s\", \"locationId\":\"%s\", \"timestamp\":\"%s\"}",
//                cardId, locationId, timestamp);
//
//        ProducerRecord<String, String> record = new ProducerRecord<>(topic, cardId, messageValue);
//
//        try {
//            Future<RecordMetadata> metadata = producer.send(record);
//            System.out.printf("Message sent to topic %s: %s%n", topic, metadata.get());
//        } catch (Exception e) {
//            System.err.printf("Error sending message: %s%n", e.getMessage());
//        }
//    }
//
//    public void close() {
//        producer.close();
//    }
}
