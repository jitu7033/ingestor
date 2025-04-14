package com.example.ingestor.model;

import lombok.Data;

@Data
public class ClickHouseConnectionDetails {
    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private String jwtToken;
}
