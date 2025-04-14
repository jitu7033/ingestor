package com.example.ingestor.controller;

import com.example.ingestor.model.IngestionRequest;
import com.example.ingestor.model.IngestionResult;
import com.example.ingestor.service.ClickHouseService;
import com.example.ingestor.service.FlatFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/***
import define dependencies for the controller including model classes for request or response
services classes for business logic spring web enable annotation enable rest api functionality
 **/


// Marks this class as a REST controller, handling HTTP requests under the /api/ingestion base path.

@RestController
@RequestMapping("/api/ingestion")
public class IngestionController {
    // Autowires service beans to handle ClickHouse and Flat file operation
    // dependency injection ensure loose coupling and testability

    @Autowired
    private ClickHouseService clickHouseService;
    @Autowired
    private FlatFileService flatFileService;

    // Get endPoint to test clickHouse connection ;
    @GetMapping("/test-connection")
    public String testConnection(){
        return clickHouseService.testConnection();
    }

    @GetMapping("/tables")


    // GET endpoint to retrieve all table names from ClickHouse.
    public ResponseEntity<?>getTables(){
        try{
            return ResponseEntity.ok(clickHouseService.getTables());
        }
        catch (Exception e){
            return ResponseEntity.status(500).body("Error Fetching Tables " + e.getMessage());
        }
    }


    // get end point to retrieve all the table name with the respect of column
    @GetMapping("/columns/{tableName}")
    public ResponseEntity<?>getColumns(@PathVariable String tableName){
        try{
            return ResponseEntity.ok(clickHouseService.getColumns(tableName));
        }
        catch(Exception e){
            return ResponseEntity.status(500).body("Error Fetching Columns : " + e.getMessage());
        }
    }

    // post endpoint to send the data from click house to flatFile csv
    @PostMapping("/clickhouse-to-flatfile")
    public ResponseEntity<?>clickHouseTOFLatFile(@RequestBody IngestionRequest request){
        try{
            long count = clickHouseService.clickHouseToFlatFile(
                    request.getTableName(),
                    request.getColumns(),
                    request.getFileName(),
                    request.getDelimiter()
            );

            return ResponseEntity.ok(new IngestionResult(count,"Ingestion Completed"));
        }
        catch (Exception e){
            return ResponseEntity.status(500).body("Error : " + e.getMessage());
        }
    }

    // post endpoint to send the data from flatFile csv to clickHouse

    @PostMapping("/flatfile-to-clickhouse")
    public ResponseEntity<?>flatFileToClickHouse(@RequestBody IngestionRequest request){
        try{
            long count = flatFileService.flatFileToClickHouse(
                    request.getFileName(),
                    request.getDelimiter(),
                    request.getTableName(),
                    request.getColumns()
            );

            return ResponseEntity.ok(new IngestionResult(count,"ingestion completed"));
        }
        catch (Exception e){
            return ResponseEntity.status(500).body("Error :" + e.getMessage());
        }
    }
}
