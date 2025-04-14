package com.example.ingestor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ClickhouseFlatfileIngestorApplication {
	public static void main(String[] args) {
		System.out.println("project setUP");
		SpringApplication.run(ClickhouseFlatfileIngestorApplication.class, args);
	}

}
