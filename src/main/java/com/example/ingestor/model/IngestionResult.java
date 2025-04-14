package com.example.ingestor.model;


import com.example.ingestor.controller.IngestionController;
import lombok.Data;
import lombok.Getter;

@Getter
@Data
public class IngestionResult {
    private long recordCount;
    private String message;

    public IngestionResult(long recordCount,String message){
        this.recordCount = recordCount;
        this.message = message;
    }


}
