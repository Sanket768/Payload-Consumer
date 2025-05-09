package com.pinnacle.payloadconsumer.consumer;

import com.pinnacle.payloadconsumer.dto.MessageDto;
import com.pinnacle.payloadconsumer.exception.PayloadParsingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinnacle.payloadconsumer.service.MessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

@Component
public class KafkaConsumer {

    private final MessageService messageService;

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Autowired
    public KafkaConsumer(MessageService messageService) {
        this.messageService = messageService;
    }

    @KafkaListener(topics = "wabanumber", groupId = "your-group-id", containerFactory = "kafkaListenerContainerFactory")
    @Async
    public void listen(List<ConsumerRecord<String, String>> records) {
        List<MessageDto> messageList = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();// convert json to object

        for (ConsumerRecord<String, String> record : records) {
            try {
                JsonNode root = mapper.readTree(record.value());
                String payload = root.path("PAYLOAD").asText();
                String logData = root.path("LOG_DATA").asText();
                String messageId = root.path("MESSAGEID").asText();

                messageList.add(new MessageDto(
                        null,
                        logData,
                        messageId,
                        payload
                ));
            }
            catch (PayloadParsingException pe) {
                throw new PayloadParsingException("Kafka message parsing failed: " + record.value(), pe);
            }
            catch (Exception e) {
                System.err.println("Skipping record due to parsing error: " + e.getMessage());
                logger.error("Skipping record due to parsing error: {}", record.value(), e);
            }
        }

        if (!messageList.isEmpty()) {
            messageService.saveMessages(messageList);
        }
    }
}