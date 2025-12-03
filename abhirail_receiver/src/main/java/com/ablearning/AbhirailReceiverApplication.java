package com.ablearning;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class AbhirailReceiverApplication {

  public static void main(String[] args) {
    SpringApplication.run(AbhirailReceiverApplication.class, args);
  }
  @KafkaListener(id = "ABR_A2B_1", topics = "AbhiRail_CityA_To_CityB")
  public void getCurrentSpeed(String message){

    System.out.println("message received is: \n"+message);
  }
}
