package com.pinnacle.payloadconsumer.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinnacle.payloadconsumer.service.MessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private final MessageService messageService;

    @Autowired
    public KafkaConsumer(MessageService messageService) {
        this.messageService = messageService;
    }

    @KafkaListener(topics = "mobile-topic-40d932ac-d426-4c89-8190-0578bad3d717", groupId = "your-group-id")
    public void listen(ConsumerRecord<String, String> record) {
        try {
            String payloadRaw = record.value();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(payloadRaw);
            String payload = root.path("PAYLOAD").asText();

            JsonNode payloadNode = mapper.readTree(payload);
            String wabanumber = payloadNode
                    .path("entry").get(0)
                    .path("changes").get(0)
                    .path("value")
                    .path("metadata")
                    .path("display_phone_number").asText();

            String logData = root.path("LOG_DATA").asText();
            String messageId = root.path("MESSAGEID").asText();

            messageService.saveMessage(wabanumber, logData, messageId, root.path("PAYLOAD").asText());

        } catch (Exception e) {
            System.err.println("Error consuming Kafka message: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
