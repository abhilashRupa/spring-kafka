package com.ablearning;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class AbhirailSenderApplication {

  public static void main(String[] args) {
    SpringApplication.run(AbhirailSenderApplication.class, args);
  }

  // creating Topic to send our messages
  @Bean
  public NewTopic createTrainStatusTopic() {
    return TopicBuilder.name("AbhiRail_CityA_To_CityB").partitions(1).replicas(1).build();
  }

  // Kafka producer sending the message
  @Bean
  public ApplicationRunner sendTrainStatusMessage(KafkaTemplate<String, String> kafkaTemplate) {

    return args -> {
      for (int i = 0; i < 500; i++) {
        double currentSpeed = Math.random() * 100;

        String currentSpeedDesc =
            (i + 1) + " instance of train's current speed is " + currentSpeed + " kmph";

        kafkaTemplate.send("AbhiRail_CityA_To_CityB", currentSpeedDesc);
        System.out.println("speed info sent successfully for " + (i + 1) + " of 500 times");
        Thread.sleep(3000, 0);
      }
    };
  }
}
