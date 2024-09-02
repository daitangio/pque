package com.gioorgi.pigi;

import javax.annotation.PostConstruct;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableAsync
@Slf4j
public class Application {
    
    @PostConstruct
    public void weAreHere(){
        log.info("$$$$$$$$$$$$$$$$ PIGI is here $$$$$$$$$$$$$$$$");
    }

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}
}
