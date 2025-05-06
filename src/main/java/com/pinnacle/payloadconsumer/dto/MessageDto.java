package com.pinnacle.payloadconsumer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageDto {
    private String wabanumber;
    private String logData;
    private String messageId;
    private String payload;


}
