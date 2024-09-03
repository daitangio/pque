package com.gioorgi.pque;

import java.util.concurrent.Executor;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableAsync
@EnableScheduling
@Slf4j
public class Application implements AsyncConfigurer {

    @Autowired
    MarketConsumer marketConsumer;

    @PostConstruct
    public void weAreHere() {
        log.info("$$$$$$$$$$$$$$$$ PIGI is here $$$$$$$$$$$$$$$$");
        log.info("$$$--- {}", marketConsumer);
    }

    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(10);
        //executor.setQueueCapacity(10000);
        executor.setThreadNamePrefix("AsycExecutor-");
        executor.initialize();
        return executor;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
