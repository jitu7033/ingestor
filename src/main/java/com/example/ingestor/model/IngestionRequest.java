package com.example.ingestor.model;

import lombok.Data;

import java.util.List;
@Data
public class IngestionRequest {
    private String tableName;
    private List<String> columns;
    private String fileName;
    private String delimiter;

    public String getDelimiter() {
        return "";
    }
}
