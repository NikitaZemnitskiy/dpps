package com.zemnitskiy.dpps.config;

import com.zemnitskiy.dpps.util.DateTimes;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.time.LocalDateTime;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    /**
     * Query parameters use exactly the same date-time format as the CSV files. A blank value is rejected rather
     * than treated as absent: {@code DELETE ?from=&to=} must not turn into "delete everything".
     */
    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverter(String.class, LocalDateTime.class, DateTimes::parse);
    }
}
