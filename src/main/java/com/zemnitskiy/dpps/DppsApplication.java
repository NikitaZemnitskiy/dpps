package com.zemnitskiy.dpps;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DppsApplication {

    public static void main(String[] args) {
        SpringApplication.run(DppsApplication.class, args);
    }

}
