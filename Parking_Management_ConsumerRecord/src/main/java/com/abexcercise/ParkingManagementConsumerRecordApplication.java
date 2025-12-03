package com.abexcercise;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class ParkingManagementConsumerRecordApplication {

  public static void main(String[] args) {
    SpringApplication.run(ParkingManagementConsumerRecordApplication.class, args);
  }

  @Bean
  public NewTopic availableParkingSpaceTopic() {
    return TopicBuilder.name("live_avbl_empty_space_cnsmr_rcd").partitions(6).replicas(1).build();
  }

  @Bean
  public ApplicationRunner produceAvailableSpace(KafkaTemplate<String, String> kafkaTemplate) {
    return args -> {
      while (true) {
        System.out.println("----------------senderModule start-------------------");
        String availableParkingSpace = String.valueOf((int) (Math.random() * 100));
        kafkaTemplate.send("live_avbl_empty_space_cnsmr_rcd", "space", availableParkingSpace);
        System.out.println("Producer available space: " + availableParkingSpace);
        Thread.sleep(3000, 0);
        System.out.println("----------------senderModule end-------------------");
      }
    };
  }

  @KafkaListener(id = "AVBL-SPC-CNSMR-RCD", topics = "live_avbl_empty_space_cnsmr_rcd")
  public void consumeAvailableSpace(ConsumerRecord<String, String> consumerRecord){

    System.out.println("--------------------Listener module start----------------------------");

    System.out.println("Topic name: "+consumerRecord.topic());
    System.out.println("timestamp: "+consumerRecord.timestamp());
    System.out.println("partition: "+consumerRecord.partition());
    System.out.println("offeset: "+consumerRecord.offset());

    System.out.println("key: "+consumerRecord.key());
    System.out.println("value: "+consumerRecord.value());

    System.out.println("--------------------Listener module end----------------------------");

  }

}
