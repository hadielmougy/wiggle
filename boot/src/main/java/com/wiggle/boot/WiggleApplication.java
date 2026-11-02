package com.wiggle.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** A wiggle server node as a Spring Boot application, configured by the same {@code WIGGLE_*} settings. */
@SpringBootApplication(proxyBeanMethods = false)
public class WiggleApplication {

    public static void main(String[] args) {
        SpringApplication.run(WiggleApplication.class, args);
    }
}
