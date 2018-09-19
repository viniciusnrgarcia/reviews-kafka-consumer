package com.reviews;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.reviews.config.KafkaConsumerConfiguration;

@SpringBootApplication
@ComponentScan(basePackageClasses = { ConsumerApplication.class, KafkaConsumerConfiguration.class })
public class ConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }
}
