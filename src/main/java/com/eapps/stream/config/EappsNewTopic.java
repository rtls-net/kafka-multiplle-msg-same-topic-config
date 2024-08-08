package com.eapps.stream.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EappsNewTopic {

	   @Value("${kafka.topic.request}")
	    private String topicName;

	    @Bean
	    public NewTopic createTopic() {
	        return new NewTopic(topicName, 3, (short) 1);
	    }
}
