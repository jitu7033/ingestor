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
 * This service supports the ingestion tool's requirement to move data from ClickHouse to Flat Files.
 */
@Service
public class ClickHouseService {

    // Autowired DataSource for ClickHouse database connectivity.
    @Autowired
    private DataSource dataSource;
    private ClickHouseConnectionDetails connectionDetails;

    // setter for dynamic configuration (injected or set via ui)

    public void setConnectionDetails(ClickHouseConnectionDetails connectionDetails){
        this.connectionDetails = connectionDetails;
        this.dataSource = null;
    }


    // lazy initialize with data source or with jwt token

    private DataSource getDataSource(){
        if(dataSource == null &&  connectionDetails != null){
            try{
                String url = "jdbc:clickhouse://" + connectionDetails.getHost() + ":" + connectionDetails.getPort()
                        + "/" + connectionDetails.getDatabase() + "?compress=0";

                if(connectionDetails.getJwtToken()!=null && !connectionDetails.getJwtToken().isEmpty()){
                    url += "&jwt=" + connectionDetails.getJwtToken(); // add jwt token
                }

                String finalUrl = url;
                dataSource = new javax.sql.DataSource(){
                    @Override
                    public <T> T unwrap(Class<T> iface) throws SQLException {
                        return null;
                    }

                    @Override
                    public boolean isWrapperFor(Class<?> iface) throws SQLException {
                        return false;
                    }

                    @Override
                    public Connection getConnection() throws java.sql.SQLException{
                        return DriverManager.getConnection(finalUrl,connectionDetails.getUsername(),connectionDetails.getPassword());
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
                    public void setLogWriter(PrintWriter out) throws SQLException {

                    }

                    @Override
                    public void setLoginTimeout(int seconds) throws SQLException {

                    }

                    @Override
                    public int getLoginTimeout() throws SQLException {
                        return 0;
                    }

                    @Override
                    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
                        return null;
                    }
                };

            }
            catch (Exception e){
                throw new RuntimeException("Failed to configure data sources " + e.getMessage());
            }
        }
        return dataSource;
    }




    /**
     * Tests connectivity to the ClickHouse database.
     * Executes a simple query to verify the connection and returns the database product name.
     *
     * @return A string indicating connection success or failure with details.
     */
    public String testConnection() {
        try (Connection conn = dataSource.getConnection()) {
            return "Connection successful: " + conn.getMetaData().getDatabaseProductName();
        } catch (Exception e) {
            return "Connection failed: " + e.getMessage();
        }
    }

    /**
     * Retrieves a list of table names from the 'uk_price_paid' database in ClickHouse.
     * Uses the SHOW TABLES query to fetch schema information.
     *
     * @return A list of table names.
     * @throws Exception If the database query fails.
     * @note Hardcodes 'uk_price_paid' database; consider making dynamic with connection details.
     */
    public List<String> getTables() throws Exception {
        List<String> tables = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES FROM uk_price_paid")) {

            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    /**
     * Retrieves column names for a specified table in ClickHouse.
     * Uses the DESCRIBE TABLE query to fetch column metadata.
     *
     * @param tableName The name of the table to inspect.
     * @return A list of column names.
     * @throws Exception If the database query fails or tableName is invalid.
     * @warning Table name is not sanitized; vulnerable to SQL injection.
     */
    public List<String> getTableColumns(String tableName) throws Exception {
        List<String> columns = new ArrayList<>();
        try (Connection conn = getDataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("DESCRIBE TABLE " + tableName)) {
            while (rs.next()) {
                columns.add(rs.getString("name"));
            }
        }
        return columns;
    }

    /**
     * Exports data from a ClickHouse table to a CSV file.
     * Queries the specified table for selected columns and writes the result to a Flat File.
     *
     * @param tableName The name of the ClickHouse table to query.
     * @param columns   The list of column names to include in the CSV.
     * @param fileName  The output CSV file path.
     * @param delimiter The delimiter to use in the CSV (e.g., "," or ";").
     * @return The number of records written to the CSV.
     * @throws Exception If the query fails, file writing fails, or inputs are invalid.
     * @warning Table name and columns are not sanitized; vulnerable to SQL injection.
     * @warning File path is not secured; consider restricting to a safe directory.
     */
    public long clickHouseToFlatFile(String tableName, List<String> columns, String fileName, String delimiter) throws Exception {
        if (columns.isEmpty()) throw new IllegalArgumentException("No columns selected");
        String query = "SELECT " + String.join(", ", columns) + " FROM " + tableName;

        try (Connection conn = getDataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             java.io.FileWriter fileWriter = new java.io.FileWriter(fileName);
             CSVWriter writer = new CSVWriter(
                     fileWriter,
                     delimiter.charAt(0), // CSV delimiter (e.g., comma).
                     '"',                // Quote character for values.
                     '\\',               // Escape character for special chars.
                     "\n"                // Line ending for rows.
             )) {
            long count = 0;
            // Write column headers as the first row.
            writer.writeNext(columns.toArray(new String[0]));

            // Process each row from the result set.
            while (rs.next()) {
                String[] row = new String[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    row[i] = rs.getString(i + 1);
                }
                // Write row to CSV.
                writer.writeNext(row);
                count++;
            }
            return count;
        }
    }

    public  long clickHouseJoinFlatFile(List<String> tables, String joinCondition, List<String> columns, String fileName, String delimiter) {
        if(tables.size() < 2) throw new IllegalArgumentException("At least Two table for join ");
        String query = "SELECT " + String.join(", ",columns) + "FROM " + tables.get(0);
        for(int i = 1; i < tables.size(); i++){
            query += " JOIN " + tables.get(i) + " ON " + joinCondition;
        }
        try(Connection conn = getDataSource().getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            java.io.FileWriter fileWriter = new java.io.FileWriter(fileName);
            com.opencsv.CSVWriter writer = new com.opencsv.CSVWriter(
                    fileWriter,
                    delimiter.charAt(0),
                    '"',
                    '\\',
                    "\n")){
            long count = 0;

            writer.writeNext(columns.toArray(new String[0]));
            while(rs.next()){
                String[] row = new String[columns.size()];
                for(int i = 0; i < columns.size(); i++){
                    row[i] = rs.getString(i + 1);
                }
                writer.writeNext(row);
                count++;
            }

            return count;
        }catch (Exception e){
            return 0;
        }
    }

}