package com.pinnacle.payloadconsumer.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinnacle.payloadconsumer.service.MessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import java.util.List;
import static org.mockito.Mockito.*;

public class KafkaConsumerTest {

    @InjectMocks
    private KafkaConsumer kafkaConsumer;

    @Mock
    private MessageService messageService;

    @Mock
    private ObjectMapper objectMapper;

    @BeforeEach
    public void init() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testListen_validPayload() throws Exception {

        String jsonPayload = "{\"PAYLOAD\":\"somePayload\", \"LOG_DATA\":\"logData\", \"MESSAGEID\":\"msg123\"}";
        ConsumerRecord<String, String> record = new ConsumerRecord<>("wabanumber", 0, 0, null, jsonPayload);

        JsonNode rootNode = mock(JsonNode.class);
        JsonNode payloadNode = mock(JsonNode.class);
        JsonNode logDataNode = mock(JsonNode.class);
        JsonNode messageIdNode = mock(JsonNode.class);

        when(objectMapper.readTree(record.value())).thenReturn(rootNode);
        when(rootNode.path("PAYLOAD")).thenReturn(payloadNode);
        when(rootNode.path("LOG_DATA")).thenReturn(logDataNode);
        when(rootNode.path("MESSAGEID")).thenReturn(messageIdNode);
        when(payloadNode.asText()).thenReturn("somePayload");
        when(logDataNode.asText()).thenReturn("logData");
        when(messageIdNode.asText()).thenReturn("msg123");

        kafkaConsumer.listen(List.of(record));

        verify(messageService, times(1)).saveMessages(anyList());
    }

    @Test
    public void testListen_invalidPayload() throws Exception {

        String invalidJsonPayload = "{INVALID_JSON}";
        ConsumerRecord<String, String> record = new ConsumerRecord<>("wabanumber", 0, 0, null, invalidJsonPayload);
        kafkaConsumer.listen(List.of(record));
        verify(messageService, never()).saveMessages(anyList());
    }
}