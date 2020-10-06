package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 程序启动时创建Topic
 */
@Configuration
public class KafkaConfig {

    @Value("${kafka.num.partitions}")
    private Integer partitions;

    @Value("${kafka.num.replica}")
    private Short replica;


    @Bean
    public NewTopic topic2(){
        return new NewTopic("tplic-k1",partitions,replica);
    }


}
