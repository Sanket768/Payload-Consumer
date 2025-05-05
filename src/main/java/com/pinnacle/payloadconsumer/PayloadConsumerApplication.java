package com.pinnacle.payloadconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class PayloadConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(PayloadConsumerApplication.class, args);
	}
}