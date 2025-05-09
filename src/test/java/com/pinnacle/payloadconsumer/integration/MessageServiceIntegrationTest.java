package com.pinnacle.payloadconsumer.integration;

import com.pinnacle.payloadconsumer.dto.MessageDto;
import com.pinnacle.payloadconsumer.service.MessageService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
public class MessageServiceIntegrationTest {

    @Autowired
    private MessageService messageService;

    @Test
    public void testSaveMessages_createsTableAndInsertsData() {
        MessageDto dto = new MessageDto();
        //dto.setPayload("{\"entry\":[{\"changes\":[{\"value\":{\"metadata\":{\"display_phone_number\":\"+1234567890\"},\"messages\":[{\"id\":\"msg456\"}]}}]}]");
        dto.setPayload("{\"entry\":[{\"changes\":[{\"value\":{\"metadata\":{\"display_phone_number\":\"+1234567890\"},\"messages\":[{\"id\":\"msg123\"}]}}]}]}");

        assertDoesNotThrow(() -> messageService.saveMessages(List.of(dto)));
    }
}
