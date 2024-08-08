package com.eapps.stream.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.eapps.stream.service.RequestService;

@RestController
public class RequestController {
	
	@Autowired
	private RequestService reqService;

	@PostMapping("publishMsgToRequest")
	public ResponseEntity<Map<String, String>> publishMsgToRequest(@RequestBody String msg) {
		System.out.println("Recieved in stream original:"+msg);
		reqService.publishMsgToRequest(msg);
		Map<String,String> res = new HashMap<String,String>();
		res.put("status", String.valueOf(200));
		res.put("msg", "request processed successfully");
		return ResponseEntity.ok().body(res);
	}
	

	
}
