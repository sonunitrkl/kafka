package com.learnkafka.kafkalearning.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * @author Sonu Kumar
 */
@Configuration
@Profile("local")
public class AutoCreateConfig {
    @Bean
    public NewTopic libraryEvents() {
        return TopicBuilder.name("Library-events")
                .partitions(3)
                .replicas(3)
                .build();
    }
}
