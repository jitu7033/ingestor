package com.example.ingestor.service;
import java.util.List;

public class ClickHouseService {

    public String testConnection() {
        return "";
    }

    public Object getTables() {

        return null;
    }

    public Object getColumns(String tableName) {
        return null;
    }

    public long clickHouseToFlatFile(String tableName,List<String>columns,String fileName,String delimiter){
        // implement
        return 0L;
    }
}

