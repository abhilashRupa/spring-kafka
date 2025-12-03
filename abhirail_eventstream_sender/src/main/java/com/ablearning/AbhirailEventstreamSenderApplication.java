package com.ablearning;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class AbhirailEventstreamSenderApplication {

  public static void main(String[] args) {
    SpringApplication.run(AbhirailEventstreamSenderApplication.class, args);
  }

  @Bean
  public NewTopic railStatusTopicBuilder() {
    return TopicBuilder.name("abhiRail_C_To_D")
            .partitions(6)
            .replicas(1).build();
  }

  @Bean
  public NewTopic railBreakEfficiencyTopicBuilder() {
    return TopicBuilder.name("abhiRail_C_To_D_Brake_Warning")
            .partitions(6)
            .replicas(1)
            .build();
  }

  @Bean
  public ApplicationRunner publishRailStatus(KafkaTemplate<String, String> kafkaTemplate){

    return args -> {
      for (int i = 0 ; i < 100 ; i++){
        String speed = String.valueOf(Math.round(Math.random()*1000));
        String breakEfficiency = String.valueOf(Math.round(Math.random()*100));

        kafkaTemplate.send("abhiRail_C_To_D", "Speed", speed);
        kafkaTemplate.send("abhiRail_C_To_D", "BrakeEfficiency", breakEfficiency);
        System.out.println("sending speed = " + speed + " Kmph and Brake efficiency = "+ breakEfficiency+ " %. ");
        Thread.sleep(3000, 0);
      }
    };

  }

}
