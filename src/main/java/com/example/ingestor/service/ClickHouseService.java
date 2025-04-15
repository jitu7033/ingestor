
package com.example.ingestor.service;

import com.example.ingestor.model.ClickHouseConnectionDetails;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.opencsv.CSVWriter;

/**
 * Service class for interacting with ClickHouse database.
 * Provides methods to test connections, retrieve schema information, and export data to CSV files.
 * Supports dynamic configuration via UI inputs (Host, Port, Database, User, Password, JWT Token)
 * and JWT authentication for the ingestion tool's requirement to move data from ClickHouse to Flat Files.
 */
@Service
public class ClickHouseService {

    // Autowired DataSource for ClickHouse database connectivity, managed by Spring
    @Autowired
    private DataSource dataSource;

    private ClickHouseConnectionDetails connectionDetails;

    /**
     * Sets dynamic connection details from UI input.
     * @param connectionDetails Object containing host, port, database, username, password, and JWT token.
     */
    public void setConnectionDetails(ClickHouseConnectionDetails connectionDetails) {
        this.connectionDetails = connectionDetails;
        // Do not set dataSource to null; rely on getEffectiveDataSource for dynamic config
    }

    /**
     * Returns an effective DataSource, using Spring-injected DataSource unless overridden by dynamic details.
     * @return Configured DataSource instance.
     * @throws RuntimeException if custom DataSource configuration fails.
     */
    private DataSource getEffectiveDataSource() {
        if (connectionDetails != null) {
            try {
                String url = "jdbc:clickhouse://" + connectionDetails.getHost() + ":" + connectionDetails.getPort()
                        + "/" + connectionDetails.getDatabase() + "?compress=0";
                if (connectionDetails.getJwtToken() != null && !connectionDetails.getJwtToken().isEmpty()) {
                    url += "&jwt=" + connectionDetails.getJwtToken(); // Add JWT token to URL
                }
                String finalUrl = url;
                return new javax.sql.DataSource() {
                    @Override
                    public Connection getConnection() throws SQLException {
                        return DriverManager.getConnection(finalUrl, connectionDetails.getUsername(), connectionDetails.getPassword());
                    }

                    @Override
                    public Connection getConnection(String username, String password) throws SQLException {
                        return null;
                    }

                    @Override
                    public PrintWriter getLogWriter() throws SQLException {
                        return null;
                    }

                    @Override
                    public void setLogWriter(PrintWriter out) throws SQLException {}

                    @Override
                    public void setLoginTimeout(int seconds) throws SQLException {}

                    @Override
                    public int getLoginTimeout() throws SQLException {
                        return 0;
                    }

                    @Override
                    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
                        return null;
                    }

                    @Override
                    public <T> T unwrap(Class<T> iface) throws SQLException {
                        return null;
                    }

                    @Override
                    public boolean isWrapperFor(Class<?> iface) throws SQLException {
                        return false;
                    }
                };
            } catch (Exception e) {
                throw new RuntimeException("Failed to configure DataSource: " + e.getMessage());
            }
        }
        return dataSource; // Fall back to Spring-injected DataSource
    }

    /**
     * Tests connectivity to the ClickHouse database.
     * Uses the effective DataSource to verify the connection and returns the database product name.
     * @return A string indicating connection success or failure with details.
     */
    public String testConnection() {
        try (Connection conn = getEffectiveDataSource().getConnection()) {
            return "Connection successful: " + conn.getMetaData().getDatabaseProductName();
        } catch (Exception e) {
            return "Connection failed: " + e.getMessage();
        }
    }

    /**
     * Retrieves a list of table names from the configured or default database in ClickHouse.
     * Uses the SHOW TABLES query to fetch schema information.
     * @return A list of table names.
     * @throws Exception If the database query fails.
     */
    public List<String> getTables() throws Exception {
        List<String> tables = new ArrayList<>();
        String database = (connectionDetails != null) ? connectionDetails.getDatabase() : "uk_price_paid";
        try (Connection conn = getEffectiveDataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES FROM " + database)) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    /**
     * Retrieves column names for a specified table in ClickHouse.
     * Uses the DESCRIBE TABLE query to fetch column metadata.
     * @param tableName The name of the table to inspect (should be sanitized in production).
     * @return A list of column names.
     * @throws Exception If the database query fails or tableName is invalid.
     */
    public List<String> getTableColumns(String tableName) throws Exception {
        List<String> columns = new ArrayList<>();
        try (Connection conn = getEffectiveDataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("DESCRIBE TABLE " + escapeIdentifier(tableName))) { // Add basic escaping
            while (rs.next()) {
                columns.add(rs.getString("name"));
            }
        }
        return columns;
    }

    /**
     * Exports data from a ClickHouse table to a CSV file.
     * Queries the specified table for selected columns and writes the result to a Flat File.
     * @param tableName The name of the ClickHouse table to query.
     * @param columns   The list of column names to include in the CSV.
     * @param fileName  The output CSV file path.
     * @param delimiter The delimiter to use in the CSV (e.g., "," or ";").
     * @return The number of records written to the CSV.
     * @throws Exception If the query fails, file writing fails, or inputs are invalid.
     */
    public long clickHouseToFlatFile(String tableName, List<String> columns, String fileName, String delimiter) throws Exception {
        if (columns.isEmpty()) throw new IllegalArgumentException("No columns selected");
        String query = "SELECT " + String.join(", ", columns.stream().map(this::escapeIdentifier).toArray(String[]::new))
                + " FROM " + escapeIdentifier(tableName);
        try (Connection conn = getEffectiveDataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             java.io.FileWriter fileWriter = new java.io.FileWriter(fileName);
             CSVWriter writer = new CSVWriter(
                     fileWriter,
                     delimiter.charAt(0),
                     '"',
                     '\\',
                     "\n")) {
            long count = 0;
            writer.writeNext(columns.toArray(new String[0]));
            while (rs.next()) {
                String[] row = new String[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    row[i] = rs.getString(i + 1);
                }
                writer.writeNext(row);
                count++;
            }
            return count;
        }
    }

    /**
     * Exports data from a ClickHouse join operation to a CSV file.
     * Performs a JOIN across multiple tables based on the provided condition.
     * @param tables The list of table names to join.
     * @param joinCondition The JOIN condition (e.g., "table1.id = table2.id").
     * @param columns The list of column names to include in the CSV.
     * @param fileName The output CSV file path.
     * @param delimiter The delimiter to use in the CSV.
     * @return The number of records written to the CSV.
     * @throws Exception If the query fails, file writing fails, or inputs are invalid.
     */
    public long clickHouseJoinToFlatFile(List<String> tables, String joinCondition, List<String> columns, String fileName, String delimiter) throws Exception {
        if (tables.size() < 2) throw new IllegalArgumentException("At least two tables required for join");
        String query = "SELECT " + String.join(", ", columns.stream().map(this::escapeIdentifier).toArray(String[]::new))
                + " FROM " + escapeIdentifier(tables.get(0));
        for (int i = 1; i < tables.size(); i++) {
            query += " JOIN " + escapeIdentifier(tables.get(i)) + " ON " + joinCondition;
        }
        try (Connection conn = getEffectiveDataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             java.io.FileWriter fileWriter = new java.io.FileWriter(fileName);
             CSVWriter writer = new CSVWriter(
                     fileWriter,
                     delimiter.charAt(0),
                     '"',
                     '\\',
                     "\n")) {
            long count = 0;
            writer.writeNext(columns.toArray(new String[0]));
            while (rs.next()) {
                String[] row = new String[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    row[i] = rs.getString(i + 1);
                }
                writer.writeNext(row);
                count++;
            }
            return count;
        }
    }


    public List<List<String>> getTableData(String tableName, List<String> columns) throws Exception {
        List<List<String>> data = new ArrayList<>();
        String query = "SELECT " + String.join(", ", columns) + " FROM " + tableName;
        try (Connection conn = getEffectiveDataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            int columnCount = columns.size();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getString(i));
                }
                data.add(row);
            }
        }
        return data;
    }


    /**
     * Escapes an identifier (e.g., table or column name) to prevent SQL injection.
     * Wraps the identifier in backticks.
     * @param identifier The identifier to escape.
     * @return The escaped identifier.
     */
    private String escapeIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }



}