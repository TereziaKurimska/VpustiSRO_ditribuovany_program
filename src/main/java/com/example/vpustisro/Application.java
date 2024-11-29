package com.example.vpustisro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Application {

        public static void main(String[] args) {
            SpringApplication.run(Application.class, args);
//
//            String kafkaBroker = "localhost:9092";
//            String topic = "AccessEvent";
        }

    }