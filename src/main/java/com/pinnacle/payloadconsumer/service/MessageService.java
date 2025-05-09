//package com.pinnacle.payloadconsumer.service;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.pinnacle.payloadconsumer.dto.MessageDto;
//import jakarta.persistence.EntityManager;
//import jakarta.persistence.PersistenceContext;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.time.LocalDate;
//import java.time.format.DateTimeFormatter;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.stream.Collectors;
//
//@Service
//public class MessageService {
//
//    @PersistenceContext
//    private EntityManager entityManager;
//
//    private final ObjectMapper objectMapper = new ObjectMapper();
//    private static final ConcurrentHashMap<String, Boolean> tableCache = new ConcurrentHashMap<>();
//
//    @Transactional
//    public void saveMessages(List<MessageDto> messages) {
//        Map<String, List<MessageDto>> grouped = new HashMap<>();
//
//        for (MessageDto dto : messages) {
//            try {
//                JsonNode rootNode = objectMapper.readTree(dto.getPayload());
//                JsonNode displayPhoneNode = rootNode.path("entry").get(0)
//                        .path("changes").get(0)
//                        .path("value")
//                        .path("metadata")
//                        .path("display_phone_number");
//
//                if (displayPhoneNode.isMissingNode()) continue;
//
//                String displayPhone = displayPhoneNode.asText().replaceAll("\\D", ""); // sanitize
//
//                grouped.computeIfAbsent(displayPhone, k -> new ArrayList<>()).add(dto);
//
//            } catch (Exception e) {
//            }
//        }
//
//        String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
//
//        for (Map.Entry<String, List<MessageDto>> entry : grouped.entrySet()) {
//            String displayPhone = entry.getKey();
//            String tableName = "wa_" + displayPhone + "_" + currentDate;
//
//            if (!tableCache.containsKey(tableName)) {
//                String createTableSQL = String.format(
//                        "CREATE TABLE IF NOT EXISTS %s (" +
//                                "sr_no BIGINT AUTO_INCREMENT PRIMARY KEY, " +
//                                "wabanumber VARCHAR(255), " +
//                                "log_data VARCHAR(255), " +
//                                "message_id VARCHAR(255), " +
//                                "payload TEXT)", tableName);
//                entityManager.createNativeQuery(createTableSQL).executeUpdate();
//                tableCache.put(tableName, true);
//            }
//
//            String insertSQL = String.format(
//                    "INSERT INTO %s (wabanumber, log_data, message_id, payload) VALUES (?, ?, ?, ?)", tableName);
//
//            for (MessageDto dto : entry.getValue()) {
//                entityManager.createNativeQuery(insertSQL)
//                        .setParameter(1, dto.getWabanumber())
//                        .setParameter(2, dto.getLogData())
//                        .setParameter(3, dto.getMessageId())
//                        .setParameter(4, dto.getPayload())
//                        .executeUpdate();
//            }
//        }
//    }
//}
//
//package com.pinnacle.payloadconsumer.service;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.pinnacle.payloadconsumer.dto.MessageDto;
//import com.pinnacle.payloadconsumer.exception.DatabaseOperationException;
//import com.pinnacle.payloadconsumer.exception.MessageProcessingException;
//import com.pinnacle.payloadconsumer.exception.PayloadParsingException;
//import com.pinnacle.payloadconsumer.exception.TableCreationException;
//import jakarta.persistence.EntityManager;
//import jakarta.persistence.PersistenceContext;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//@Service
//public class MessageService {
//
//    @PersistenceContext
//    private EntityManager entityManager;
//
//    private final ObjectMapper objectMapper = new ObjectMapper();
//    private static final ConcurrentHashMap<String, Boolean> tableCache = new ConcurrentHashMap<>();
//
//    @Transactional
//    public void saveMessages(List<MessageDto> messages) {
//        Map<String, List<MessageDto>> grouped = new HashMap<>();
//        for (MessageDto dto : messages) {
//            try {
//                JsonNode rootNode = objectMapper.readTree(dto.getPayload());
//                JsonNode valueNode = rootNode.path("entry").get(0)
//                        .path("changes").get(0)
//                        .path("value");
//
//                // Extract display_phone_number
//                String displayPhone = valueNode.path("metadata").path("display_phone_number").asText("").replaceAll("\\D", "");
//                if (displayPhone.isEmpty()) continue;
//
//                // Extract message_id from either statuses or messages
//                String messageId = "";
//                JsonNode statusesNode = valueNode.path("statuses");
//                JsonNode messagesNode = valueNode.path("messages");
//
//                if (statusesNode.isArray() && statusesNode.size() > 0) {
//                    messageId = statusesNode.get(0).path("id").asText("");
//                } else if (messagesNode.isArray() && messagesNode.size() > 0) {
//                    messageId = messagesNode.get(0).path("id").asText("");
//                }
//
//                // Set extracted values to DTO
//                dto.setWabanumber(displayPhone);
//                dto.setMessageId(messageId);
//                dto.setLogData(LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")));
//
//                // Grouping for table
//                grouped.computeIfAbsent(displayPhone, k -> new ArrayList<>()).add(dto);
//
//            }
//            catch (PayloadParsingException pe) {
//                throw new PayloadParsingException("Invalid JSON structure in payload: " + dto.getPayload(), pe);
//            }catch (Exception e) {
//                throw new MessageProcessingException("Failed to parse message payload: " + dto.getPayload(), e);
//            }
//        }
//
//        String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
//
//        for (Map.Entry<String, List<MessageDto>> entry : grouped.entrySet()) {
//            String displayPhone = entry.getKey();
//            String tableName = "wa_" + displayPhone + "_" + currentDate;
//
//            if (!tableCache.containsKey(tableName)) {
//                String createTableSQL = String.format(
//                        "CREATE TABLE IF NOT EXISTS %s (" +
//                                "sr_no BIGINT AUTO_INCREMENT PRIMARY KEY, " +
//                                "wabanumber VARCHAR(255), " +
//                                "log_data VARCHAR(255), " +
//                                "message_id VARCHAR(255), " +
//                                "payload TEXT)", tableName);
//                try {
//                    entityManager.createNativeQuery(createTableSQL).executeUpdate();
//                    tableCache.put(tableName, true);
//                } catch (Exception e) {
//                    throw new TableCreationException("Failed to create table: " + tableName, e);
//                }
//            }
//
////                entityManager.createNativeQuery(createTableSQL).executeUpdate();
////                tableCache.put(tableName, true);
////            }
//
//            String insertSQL = String.format(
//                    "INSERT INTO %s (wabanumber, log_data, message_id, payload) VALUES (?, ?, ?, ?)", tableName);
//
//            for (MessageDto dto : entry.getValue()) {
//                try {
//                    entityManager.createNativeQuery(insertSQL)
//                            .setParameter(1, dto.getWabanumber())
//                            .setParameter(2, dto.getLogData())
//                            .setParameter(3, dto.getMessageId())
//                            .setParameter(4, dto.getPayload())
//                            .executeUpdate();
//                }
//                catch (DatabaseOperationException e) {
//                    throw new DatabaseOperationException("Failed to insert record into: " + tableName, e);
//                }catch (Exception e) {
//                    throw new MessageProcessingException("Failed to insert data into table: " + tableName, e);
//                }
//            }
//        }
//    }
//}

package com.pinnacle.payloadconsumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinnacle.payloadconsumer.dto.MessageDto;
import com.pinnacle.payloadconsumer.exception.DatabaseOperationException;
import com.pinnacle.payloadconsumer.exception.MessageProcessingException;
import com.pinnacle.payloadconsumer.exception.PayloadParsingException;
import com.pinnacle.payloadconsumer.exception.TableCreationException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MessageService {

    private static final Logger logger = LoggerFactory.getLogger(MessageService.class);

    @PersistenceContext
    private EntityManager entityManager;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final ConcurrentHashMap<String, Boolean> tableCache = new ConcurrentHashMap<>();

    @Transactional
    public void saveMessages(List<MessageDto> messages) {
        logger.info("Starting message processing for {} messages", messages.size());

        Map<String, List<MessageDto>> grouped = new HashMap<>();
        for (MessageDto dto : messages) {
            try {
                JsonNode rootNode = objectMapper.readTree(dto.getPayload());
                JsonNode valueNode = rootNode.path("entry").get(0)
                        .path("changes").get(0)
                        .path("value");

                String displayPhone = valueNode.path("metadata").path("display_phone_number").asText("").replaceAll("\\D", "");
                if (displayPhone.isEmpty()) {
                    logger.warn("Skipping message due to missing display_phone_number: {}", dto.getPayload());
                    continue;
                }

                String messageId = "";
                JsonNode statusesNode = valueNode.path("statuses");
                JsonNode messagesNode = valueNode.path("messages");

                if (statusesNode.isArray() && statusesNode.size() > 0) {
                    messageId = statusesNode.get(0).path("id").asText("");
                } else if (messagesNode.isArray() && messagesNode.size() > 0) {
                    messageId = messagesNode.get(0).path("id").asText("");
                }

                dto.setWabanumber(displayPhone);
                dto.setMessageId(messageId);
                dto.setLogData(LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")));

                grouped.computeIfAbsent(displayPhone, k -> new ArrayList<>()).add(dto);

                logger.debug("Parsed message for wabanumber {}: messageId={}", displayPhone, messageId);

            } catch (PayloadParsingException pe) {
                logger.error("PayloadParsingException for payload: {}", dto.getPayload(), pe);
                throw new PayloadParsingException("Invalid JSON structure in payload: " + dto.getPayload(), pe);
            } catch (Exception e) {
                logger.error("Failed to parse payload: {}", dto.getPayload(), e);
                throw new MessageProcessingException("Failed to parse message payload: " + dto.getPayload(), e);
            }
        }

        String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        for (Map.Entry<String, List<MessageDto>> entry : grouped.entrySet()) {
            String displayPhone = entry.getKey();
            String tableName = "wa_" + displayPhone + "_" + currentDate;

            if (!tableCache.containsKey(tableName)) {
                String createTableSQL = String.format(
                        "CREATE TABLE IF NOT EXISTS %s (" +
                                "sr_no BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                                "wabanumber VARCHAR(255), " +
                                "log_data VARCHAR(255), " +
                                "message_id VARCHAR(255), " +
                                "payload TEXT)", tableName);
                try {
                    entityManager.createNativeQuery(createTableSQL).executeUpdate();
                    tableCache.put(tableName, true);
                    logger.info("Created new table: {}", tableName);
                } catch (Exception e) {
                    logger.error("Table creation failed for: {}", tableName, e);
                    throw new TableCreationException("Failed to create table: " + tableName, e);
                }
            }

            String insertSQL = String.format(
                    "INSERT INTO %s (wabanumber, log_data, message_id, payload) VALUES (?, ?, ?, ?)", tableName);

        for (MessageDto dto : entry.getValue()) {
                try {
                    entityManager.createNativeQuery(insertSQL)
                            .setParameter(1, dto.getWabanumber())
                            .setParameter(2, dto.getLogData())
                            .setParameter(3, dto.getMessageId())
                            .setParameter(4, dto.getPayload())
                            .executeUpdate();
                    logger.debug("Inserted message into table {}: messageId={}", tableName, dto.getMessageId());
                } catch (DatabaseOperationException e) {
                    logger.error("DatabaseOperationException during insert into: {}", tableName, e);
                    throw new DatabaseOperationException("Failed to insert record into: " + tableName, e);
                } catch (Exception e) {
                    logger.error("Exception during insert into: {}", tableName, e);
                    throw new MessageProcessingException("Failed to insert data into table: " + tableName, e);
                }
            }
        }
        logger.info("Message processing completed successfully");
    }
}