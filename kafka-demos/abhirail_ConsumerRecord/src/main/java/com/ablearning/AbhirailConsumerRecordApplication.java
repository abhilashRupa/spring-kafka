package com.ablearning;

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
public class AbhirailConsumerRecordApplication {

  private static int listenerCount = 0;

  public static void main(String[] args) {
    SpringApplication.run(AbhirailConsumerRecordApplication.class, args);
  }

  @Bean
  public NewTopic createConsumerRecordTopic() {
    return TopicBuilder.name("InfyRail_C_To_D")
            .partitions(6)
            .replicas(1)
            .build();
  }

  @Bean
  public ApplicationRunner publishAbhiRailStatus(KafkaTemplate<String, String> kafkaTemplate){
    return args -> {
      for (int i = 0; i <= 100; i++) {
        System.out.println("----------------senderModule start-------------------");
        kafkaTemplate.send(
            "InfyRail_C_To_D", "Speed", String.valueOf(Math.round(Math.random() * 1000)));
        kafkaTemplate.send(
            "InfyRail_C_To_D", "BrakeEfficiency", String.valueOf(Math.round(Math.random() * 100)));

        System.out.println("Speed and braking efficiency information sent successfully:- "+(i+1)+" time");
        System.out.println("----------------senderModule end-------------------");

        Thread.sleep(3000, 0);
      }
    };
  }


  @KafkaListener(id = "ABC2D_1", topics = "InfyRail_C_To_D")
  public void listenAbhiRailListeners(ConsumerRecord<String, String> consumerRecord){
    System.out.println("--------------------Listener module start----------------------------");
    listenerCount++;
    System.out.println("Speed and braking efficiency information received successfully:- "+(listenerCount)+" time");
    System.out.println("Key: "+ consumerRecord.key());
    System.out.println("value: "+consumerRecord.value());
    System.out.println("partitions: "+consumerRecord.partition());
    System.out.println("timestamp: "+ consumerRecord.timestamp());
    System.out.println("topic: "+ consumerRecord.topic());
    System.out.println("offset: "+consumerRecord.offset());
    System.out.println("--------------------Listener module end----------------------------");
  }

}
