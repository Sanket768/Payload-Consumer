package com.pinnacle.payloadconsumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Service
public class MessageService {

    @PersistenceContext
    private EntityManager entityManager;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Transactional
    public void saveMessage(String wabanumber, String logData, String messageId, String payload) {
        try {
            // Parse payload and extract display_phone_number
            JsonNode rootNode = objectMapper.readTree(payload);
            JsonNode displayPhoneNode = rootNode.path("entry").get(0)
                    .path("changes").get(0)
                    .path("value")
                    .path("metadata")
                    .path("display_phone_number");

            if (displayPhoneNode.isMissingNode()) {
                throw new MessageSavingException("display_phone_number not found in payload", null);
            }

            String displayPhone = displayPhoneNode.asText();
            displayPhone = displayPhone.replaceAll("\\D", ""); // sanitize, keep digits only

            String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            String tableName = "wa_" + displayPhone + "_" + currentDate;

            String createTableSQL = String.format(
                    "CREATE TABLE IF NOT EXISTS %s (" +
                            "sr_no BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                            "wabanumber varchar(255), " +
                            "log_data VARCHAR(255), " +
                            "message_id VARCHAR(255), " +
                            "payload TEXT)", tableName);

            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            String insertSQL = String.format(
                    "INSERT INTO %s (wabanumber, log_data, message_id, payload) VALUES (?, ?, ?, ?)", tableName);

            entityManager.createNativeQuery(insertSQL)
                    .setParameter(1, wabanumber)
                    .setParameter(2, logData)
                    .setParameter(3, messageId)
                    .setParameter(4, payload)
                    .executeUpdate();
        } catch (Exception e) {
            throw new MessageSavingException("Error saving message to the database", e);
        }
    }

    public static class MessageSavingException extends RuntimeException {
        public MessageSavingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
//
//package com.pinnacle.payloadconsumer.service;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import jakarta.persistence.EntityManager;
//import jakarta.persistence.PersistenceContext;
//import jakarta.transaction.Transactional;
//import org.springframework.stereotype.Service;
//
//import java.time.LocalDate;
//import java.time.format.DateTimeFormatter;
//import java.util.ArrayList;
//import java.util.List;
//
//@Service
//public class MessageService {
//
//    @PersistenceContext
//    private EntityManager entityManager;
//
//    private final ObjectMapper objectMapper = new ObjectMapper();
//
//    private static final int BATCH_SIZE = 50;
//
//    private final ThreadLocal<List<Object[]>> batchCache = ThreadLocal.withInitial(ArrayList::new);
//    private final ThreadLocal<String> currentTable = new ThreadLocal<>();
//
//    @Transactional
//    public void saveMessage(String wabanumber, String logData, String messageId, String payload) {
//        try {
//            // Extract display_phone_number from payload
//            JsonNode rootNode = objectMapper.readTree(payload);
//            JsonNode displayPhoneNode = rootNode.path("entry").get(0)
//                    .path("changes").get(0)
//                    .path("value")
//                    .path("metadata")
//                    .path("display_phone_number");
//
//            if (displayPhoneNode.isMissingNode()) {
//                throw new MessageSavingException("display_phone_number not found in payload", null);
//            }
//
//            String displayPhone = displayPhoneNode.asText().replaceAll("\\D", "");
//            String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
//            String tableName = "wa_" + displayPhone + "_" + currentDate;
//
//            createTableIfNotExists(tableName);
//
//            List<Object[]> batch = batchCache.get();
//            batch.add(new Object[]{wabanumber, logData, messageId, payload});
//            currentTable.set(tableName);
//
//            if (batch.size() >= BATCH_SIZE) {
//                flushBatch();
//            }
//
//        } catch (Exception e) {
//            throw new MessageSavingException("Error saving message to the database", e);
//        }
//    }
//
//    @Transactional
//    public void flushBatch() {
//        List<Object[]> batch = batchCache.get();
//        String tableName = currentTable.get();
//
//        if (batch.isEmpty() || tableName == null) return;
//
//        StringBuilder sb = new StringBuilder("INSERT INTO " + tableName +
//                " (wabanumber, log_data, message_id, payload) VALUES ");
//
//        for (int i = 0; i < batch.size(); i++) {
//            sb.append("(?, ?, ?, ?)");
//            if (i < batch.size() - 1) sb.append(", ");
//        }
//
//        var query = entityManager.createNativeQuery(sb.toString());
//        int index = 1;
//        for (Object[] row : batch) {
//            for (Object col : row) {
//                query.setParameter(index++, col);
//            }
//        }
//
//        query.executeUpdate();
//        batch.clear();
//        currentTable.remove();
//    }
//
//    private void createTableIfNotExists(String tableName) {
//        String createTableSQL = String.format(
//                "CREATE TABLE IF NOT EXISTS %s (" +
//                        "sr_no BIGINT AUTO_INCREMENT PRIMARY KEY, " +
//                        "wabanumber varchar(255), " +
//                        "log_data VARCHAR(255), " +
//                        "message_id VARCHAR(255), " +
//                        "payload TEXT)", tableName);
//        entityManager.createNativeQuery(createTableSQL).executeUpdate();
//    }
//
//    public static class MessageSavingException extends RuntimeException {
//        public MessageSavingException(String message, Throwable cause) {
//            super(message, cause);
//        }
//    }
//}
