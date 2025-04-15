
package com.example.ingestor.service;

import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
public class FlatFileService {
    @Autowired
    private DataSource dataSource;



    /**
     * Ingests data from a CSV file into a specified ClickHouse table.
     * Reads the CSV file, constructs INSERT queries for the selected columns, and executes them in batches for performance.
     *
     * @param fileName   The path to the input CSV file to be ingested.
     * @param delimiter  The delimiter used in the CSV file (e.g., "," or ";").
     * @param tableName  The name of the target ClickHouse table to insert data into.
     * @param columns    The list of column names in the ClickHouse table to receive the CSV data.
     * @return The number of records processed frdocker restart clickhouse-serverom the CSV file (includes rows inserted).
     * @throws Exception If file reading fails, database operations encounter errors, or input validation fails.
     * @warning Table name and columns are not sanitized, making the query vulnerable to SQL injection.
     * @warning File path is not secured; direct use of fileName could allow access to unauthorized directories.
     * @note Assumes CSV headers exist and match the provided columns; no validation is performed.
     * @note Uses batch inserts with a threshold of 1000 rows to optimize database performance.
     */


    public long flatFileToClickHouse(String fileName, String delimiter, String tableName, List<String> columns) throws Exception {
        if (columns.isEmpty()) throw new IllegalArgumentException("No columns selected");

        // Open CSVReader for the input file and establish database connection.
        try (CSVReader reader = new CSVReader(new FileReader(fileName));
             Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            String[] headers = reader.readNext();
            if (headers == null) throw new IllegalArgumentException("Empty CSV file");
            long count = 0;
            String insertQuery = "INSERT INTO " + tableName + " (" + String.join(", ", columns) + ") VALUES ";
            List<String> values = new ArrayList<>();
            for (String[] row : reader) {
                if (row.length < columns.size()) continue;
                List<String> rowValues = new ArrayList<>();
                for (int i = 0; i < columns.size(); i++) {
                    rowValues.add("'" + row[i].replace("'", "''") + "'");
                }
                values.add("(" + String.join(", ", rowValues) + ")");
                count++;
                if (values.size() >= 1000) {
                    stmt.execute(insertQuery + String.join(", ", values));
                    values.clear();
                }
            }
            if (!values.isEmpty()) stmt.execute(insertQuery + String.join(", ", values));
            return count;
        }
    }
}