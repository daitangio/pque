package com.gioorgi.pigi;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableAsync
@EnableScheduling
@Slf4j
public class Application {
    
    @Autowired MarketConsumer marketConsumer;

    @PostConstruct
    public void weAreHere(){
        log.info("$$$$$$$$$$$$$$$$ PIGI is here $$$$$$$$$$$$$$$$");
        log.info("$$$--- {}", marketConsumer);
    }

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}
}
