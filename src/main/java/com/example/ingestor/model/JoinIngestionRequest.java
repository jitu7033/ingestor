package com.example.ingestor.model;

import java.util.List;

public class JoinIngestionRequest {
    private List<String >tables;
    private String joinCondition;
    private List<String >columns;
    private String fileName;
    private String delimiter;

    // Getter and Setter

    public List<String> getTables() { return tables; }
    public void setTables(List<String> tables) { this.tables = tables; }
    public String getJoinCondition() { return joinCondition; }
    public void setJoinCondition(String joinCondition) { this.joinCondition = joinCondition; }
    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }
    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }
    public String getDelimiter() { return delimiter; }
    public void setDelimiter(String delimiter) { this.delimiter = delimiter; }
}
