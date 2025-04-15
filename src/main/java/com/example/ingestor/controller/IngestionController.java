
package com.example.ingestor.controller;

import com.example.ingestor.model.ClickHouseConnectionDetails;
import com.example.ingestor.model.IngestionRequest;
import com.example.ingestor.model.IngestionResult;
import com.example.ingestor.model.JoinIngestionRequest;
import com.example.ingestor.service.ClickHouseService;
import com.example.ingestor.service.FlatFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

// rest controller
@RestController
@RequestMapping("/api/ingestion")
public class IngestionController {


    @Autowired
    private ClickHouseService clickHouseService;
    @Autowired
    private FlatFileService flatFileService;


    @PostMapping("/configure-connection")
    public ResponseEntity<String>configureConnection(@RequestBody ClickHouseConnectionDetails details){
        clickHouseService.setConnectionDetails(details);

        try{
            String testResult = clickHouseService.testConnection();
            return ResponseEntity.ok(testResult);
        }
        catch (Exception e){
            return ResponseEntity.status(500).body("Configuration failed : " + e.getMessage());
        }
    }

    // check connection are established or not
    @GetMapping("/test-connection")
    public String testConnection() {
        return clickHouseService.testConnection();
    }

    // get the table from the table columns
    @GetMapping("/tables")
    public ResponseEntity<?> getTables() {
        try {
            return ResponseEntity.ok(clickHouseService.getTables());
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error fetching tables: " + e.getMessage());
        }
    }

    @GetMapping("/columns/{tableName}")
    public ResponseEntity<?> getColumns(@PathVariable String tableName) {
        try {
            return ResponseEntity.ok(clickHouseService.getTableColumns(tableName));
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error fetching columns: " + e.getMessage());
        }
    }

    // convert the clickhouse to flatfile

    @PostMapping("/clickhouse-to-flatfile")
    public ResponseEntity<?> clickHouseToFlatFile(@RequestBody IngestionRequest request) {
        try {
            long count = clickHouseService.clickHouseToFlatFile(
                    request.getTableName(),
                    request.getColumns(),
                    request.getFileName(),
                    request.getDelimiter()
            );
            return ResponseEntity.ok(new IngestionResult(count, "Ingestion completed"));
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    // convert the flatfile to click house
    @PostMapping("/flatfile-to-clickhouse")
    public ResponseEntity<?> flatFileToClickHouse(@RequestBody IngestionRequest request) {
        try {
            long count = flatFileService.flatFileToClickHouse(
                    request.getFileName(),
                    request.getDelimiter(),
                    request.getTableName(),
                    request.getColumns()
            );
            return ResponseEntity.ok(new IngestionResult(count, "Ingestion completed"));
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    // post mapping clickHouse join to flatFile

    @PostMapping("clickhouse-join-to-flatfile")
    public ResponseEntity<?>clickHouseJoinToFlatFile(@RequestBody JoinIngestionRequest request){
        try{
            long count = clickHouseService.clickHouseJoinFlatFile(
                    request.getTables(),
                    request.getJoinCondition(),
                    request.getColumns(),
                    request.getFileName(),
                    request.getDelimiter()
            );
            return ResponseEntity.ok(new IngestionResult(count,"Join Ingestion completed"));

        }catch (Exception e){
            return ResponseEntity.status(500).body("Error : " + e.getMessage());
        }
    }
}