package com.ablearing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
@EnableKafkaStreams
public class AbhirailEventstreamReceiverApplication {

  public static void main(String[] args) {
    SpringApplication.run(AbhirailEventstreamReceiverApplication.class, args);
  }

  @KafkaListener(id = "ABC2D_1", topics = "abhiRail_C_To_D_Brake_Warning")
	public void consumeBreakWarning(ConsumerRecord<String, String> consumerRecord){

    System.out.println("Receiving "+consumerRecord.key()+ " as "+consumerRecord.value() + "%. ");
  }

}
