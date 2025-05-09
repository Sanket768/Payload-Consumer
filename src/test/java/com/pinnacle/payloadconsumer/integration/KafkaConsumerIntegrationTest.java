package com.pinnacle.payloadconsumer.integration;

import com.pinnacle.payloadconsumer.service.MessageService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import static org.mockito.Mockito.*;

@SpringBootTest
@Import(KafkaConsumerIntegrationTest.TestConfig.class)
public class KafkaConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private MessageService messageService;

//    @Test
//    public void testIntegration_validPayload() throws InterruptedException {
//        // Send a flat payload that the KafkaConsumer understands
//        String payload = "{\"PAYLOAD\":\"somePayload\", \"LOG_DATA\":\"logData\", \"MESSAGEID\":\"msg123\"}";
//
//        kafkaTemplate.send("wabanumber", payload);
//        Thread.sleep(2000); // Allow some time for Kafka listener to process
//
//        // Verify that the mock messageService was invoked
//        verify(messageService, atLeastOnce()).saveMessages(anyList());
//    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        public MessageService messageService() {
            return mock(MessageService.class);
        }
    }
}

