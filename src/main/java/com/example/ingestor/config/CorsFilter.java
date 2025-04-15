    //package com.example.ingestor.config;
    //
    //import org.springframework.context.annotation.Bean;
    //import org.springframework.context.annotation.Configuration;
    //import org.springframework.web.cors.CorsConfiguration;
    //import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
    //import org.springframework.web.filter.CorsFilter;
    //
    //@Configuration
    //public class CorsFilter {
    //
    //    @Bean
    //    public CorsFilter corsFilter() {
    //        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    //        CorsConfiguration config = new CorsConfiguration();
    //        config.addAllowedOrigin("http://localhost:3000"); // Allow frontend origin
    //        config.addAllowedMethod("*"); // Allow all HTTP methods
    //        config.addAllowedHeader("*"); // Allow all headers
    //        config.setAllowCredentials(true); // Allow credentials if needed
    //        source.registerCorsConfiguration("/**", config); // Apply to all endpoints
    //        return new CorsFilter(source);
    //    }
    //}