

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
import java.util.*;

@RestController
@RequestMapping("/api/ingestion")
public class IngestionController {

    @Autowired
    private ClickHouseService clickHouseService;
    @Autowired
    private FlatFileService flatFileService;

    @PostMapping("/configure-connection")
    public ResponseEntity<String> configureConnection(@RequestBody ClickHouseConnectionDetails details) {
        clickHouseService.setConnectionDetails(details);
        try {
            String testResult = clickHouseService.testConnection();
            return ResponseEntity.ok(testResult);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Configuration failed: " + e.getMessage());
        }
    }

    @GetMapping("/test-connection")
    public String testConnection() {
        return clickHouseService.testConnection();
    }

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

    @PostMapping("/ingest")
    public ResponseEntity<?> ingestData(@RequestBody IngestionRequest request) {
        try {
            long count;
            if ("ClickHouse".equalsIgnoreCase(request.getSource())) {
                count = clickHouseService.clickHouseToFlatFile(
                        request.getTableName(),
                        request.getColumns(),
                        request.getFileName(),
                        request.getDelimiter()
                );
                return ResponseEntity.ok(new IngestionResult(count, "Ingestion from ClickHouse completed"));
            } else if ("FlatFile".equalsIgnoreCase(request.getSource())) {
                count = flatFileService.flatFileToClickHouse(
                        request.getFileName(),
                        request.getDelimiter(),
                        request.getTableName(),
                        request.getColumns()
                );
                return ResponseEntity.ok(new IngestionResult(count, "Ingestion from FlatFile completed"));
            } else {
                return ResponseEntity.badRequest().body("Invalid source. Use 'ClickHouse' or 'FlatFile'");
            }
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @PostMapping("/clickhouse-join-to-flatfile")
    public ResponseEntity<?> clickHouseJoinToFlatFile(@RequestBody JoinIngestionRequest request) {
        try {
            long count = clickHouseService.clickHouseJoinToFlatFile(
                    request.getTables(),
                    request.getJoinCondition(),
                    request.getColumns(),
                    request.getFileName(),
                    request.getDelimiter()
            );
            return ResponseEntity.ok(new IngestionResult(count, "Join Ingestion completed"));
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @GetMapping("/data")
    public ResponseEntity<?> getData(@RequestParam String source, @RequestParam String tableName, @RequestParam String fileName, @RequestParam String delimiter, @RequestParam(required = false) List<String> columns) {
        try {
            List<List<String>> data;
            if ("ClickHouse".equalsIgnoreCase(source)) {
                data = clickHouseService.getTableData(tableName, columns != null ? columns : clickHouseService.getTableColumns(tableName));
            } else if ("FlatFile".equalsIgnoreCase(source)) {
                data = flatFileService.getFlatFileData(fileName, delimiter);
            } else {
                return ResponseEntity.badRequest().body("Invalid source. Use 'ClickHouse' or 'FlatFile'");
            }
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error fetching data: " + e.getMessage());
        }
    }


}