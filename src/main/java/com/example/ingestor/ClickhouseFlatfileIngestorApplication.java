package com.example.ingestor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;


@SpringBootApplication
public class ClickhouseFlatfileIngestorApplication {
	public static void main(String[] args) {
		SpringApplication.run(ClickhouseFlatfileIngestorApplication.class, args);
	}

}
