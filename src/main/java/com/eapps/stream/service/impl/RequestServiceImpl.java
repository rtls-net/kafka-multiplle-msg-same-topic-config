package com.eapps.stream.service.impl;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.eapps.stream.service.RequestService;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class RequestServiceImpl implements  RequestService{

	@Value("${kafka.topic.request}")
	private  String requestTopic;
	
	@Autowired
	private KafkaTemplate kafkaTemplate;
	
	public void publishMsgToRequest(String msg) {
		kafkaTemplate.send(requestTopic, msg);
	}
	
	 public void sendEvents(String user) {
	        try {
	            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(requestTopic, user);
	            future.whenComplete((result, ex) -> {
	                if (ex == null) {
	                    System.out.println("Sent message=[" + user.toString() +
	                            "] with offset=[" + result.getRecordMetadata().offset() + "]");
	                } else {
	                    System.out.println("Unable to send message=[" +
	                            user.toString() + "] due to : " + ex.getMessage());
	                }
	            });
	        } catch (Exception ex) {
	            System.out.println(ex.getMessage());
	        }
	    }
	
	/*@RetryableTopic(kafkaTemplate = "kafkaTemplate",
    attempts = "4",
    backoff = @Backoff(delay = 3000, multiplier = 1.5, maxDelay = 15000)*/
    
	/* @Transactional */
	
	/*@RetryableTopic(kafkaTemplate = "kafkaTemplate",
	        exclude = {DeserializationException.class,
	                MessageConversionException.class,
	                ConversionException.class,
	                MethodArgumentResolutionException.class,
	                NoSuchMethodException.class,
	                ClassCastException.class},
	        attempts = "4",
	        backoff = @Backoff(delay = 3000, multiplier = 1.5, maxDelay = 15000)
	)*/
	
    @RetryableTopic(attempts = "4")// 3 topic N-1
	//@KafkaListener(topics = {"eapps-rqst-tpk"},groupId = "eapps-grp-2")
    @KafkaListener(topics = "${kafka.topic.request}",groupId = "eapps-grp-2")
	public void processRequest(String msg, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) throws Exception {
    	 log.info("Received: {} from {} offset {}", msg, topic, offset);
		System.out.println("Listener1:"+msg);
		throw new Exception("Invalid call");
	}
	
//	//@KafkaListener(topics = {"eapps-rqst-tpk"},groupId = "eapps-grp-3")
//	@KafkaListener(topics = {"eapps-rqst-tpk"},groupId = "eapps-grp-2")
//	public void processRequest1(String msg) {
//		System.out.println("Listener in original:"+msg);
//	}
		
	@DltHandler
	public void handleDLt(String msg, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
		log.info("Dlt: {} from {} offset {}", msg, topic, offset);
		System.out.println("DLT Message:" + msg);
	}	
}
