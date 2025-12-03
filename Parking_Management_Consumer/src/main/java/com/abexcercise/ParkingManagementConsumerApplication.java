package com.abexcercise;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class ParkingManagementConsumerApplication {

  public static void main(String[] args) {
    SpringApplication.run(ParkingManagementConsumerApplication.class, args);
  }

  // Consume the available space from the kafka broker
  @KafkaListener(id = "AVBL-SPC", topics = "live_avbl_empty_space")
  public void consumeAvailableSpace(String availableParkingNumbers) {

    System.out.println("Consumer available space: " + availableParkingNumbers);
  }
}
