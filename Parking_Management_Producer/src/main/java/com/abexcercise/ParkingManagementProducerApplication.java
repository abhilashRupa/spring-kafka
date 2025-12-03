package com.abexcercise;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class ParkingManagementProducerApplication {

  public static void main(String[] args) {
    SpringApplication.run(ParkingManagementProducerApplication.class, args);
  }

  // create the topic
  @Bean
  public NewTopic createAvailableParkingSpaceTopic() {
    return TopicBuilder.name("live_avbl_empty_space").partitions(1).replicas(1).build();
  }

  // Produce the available space to the kafka broker
  @Bean
  public ApplicationRunner produceAvailableSpace(KafkaTemplate<String, String> kafkaTemplate) {

    return args -> {
      while (true) {
        String availableParkingSpace = String.valueOf((int) (Math.random() * 100));
        kafkaTemplate.send("live_avbl_empty_space", availableParkingSpace);
        System.out.println("Producer available space: " + availableParkingSpace);
        Thread.sleep(3000, 0);
      }
    };
  }
}
