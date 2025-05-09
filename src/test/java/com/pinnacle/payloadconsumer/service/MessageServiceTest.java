package com.pinnacle.payloadconsumer.service;

import com.pinnacle.payloadconsumer.dto.MessageDto;
import com.pinnacle.payloadconsumer.exception.MessageProcessingException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.startsWith;


public class MessageServiceTest {

    @InjectMocks
    private MessageService messageService;

    @Mock
    private EntityManager entityManager;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    private MessageDto validDto() {
        MessageDto dto = new MessageDto();
        dto.setPayload("{\"entry\":[{\"changes\":[{\"value\":{\"metadata\":{\"display_phone_number\":\"+1234567890\"},\"messages\":[{\"id\":\"msg123\"}]} }]}]");
        return dto;
    }

    @Test
    public void testSaveMessages_success() {
        MessageDto dto = new MessageDto();
        dto.setPayload("{\"entry\":[{\"changes\":[{\"value\":{\"metadata\":{\"display_phone_number\":\"+1234567890\"},\"messages\":[{\"id\":\"msg123\"}]}}]}]}");
        Query createTableQuery = mock(Query.class);
        Query insertQuery = mock(Query.class);
        when(entityManager.createNativeQuery(contains("CREATE TABLE"))).thenReturn(createTableQuery);
        when(entityManager.createNativeQuery(startsWith("INSERT INTO"))).thenReturn(insertQuery);

        when(createTableQuery.executeUpdate()).thenReturn(1);
        when(insertQuery.setParameter(anyInt(), any())).thenReturn(insertQuery);
        when(insertQuery.executeUpdate()).thenReturn(1);
        assertDoesNotThrow(() -> messageService.saveMessages(List.of(dto)));

        verify(entityManager, atLeastOnce()).createNativeQuery(anyString());
    }


    @Test
    public void testSaveMessages_invalidPayload() {
        MessageDto dto = new MessageDto();
        dto.setPayload("invalid_json");

        Exception ex = assertThrows(MessageProcessingException.class,
                () -> messageService.saveMessages(List.of(dto)));
        assertTrue(ex.getMessage().contains("Failed to parse message payload"));
    }
}