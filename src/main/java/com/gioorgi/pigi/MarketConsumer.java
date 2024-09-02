package com.gioorgi.pigi;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.tembo.pgmq.PGMQClient;
import io.tembo.pgmq.PGMQueue;
import io.tembo.pgmq.json.PGMQJsonProcessor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Service
@Setter
@Slf4j
@ToString
public class MarketConsumer {

    @Autowired
    PGMQClient pgmqClient;

    @Autowired
    PGMQJsonProcessor jsonProcessor;

    @Scheduled(fixedDelay = 5, timeUnit = TimeUnit.SECONDS)
    public void processMarketRequests(){
        log.trace("Checking for message...");
        var m=pgmqClient.pop(PGMQueue.builder().name("market_request").build());
        m.ifPresentOrElse( msg ->{
            log.info("{}\t{}",msg.getId(), msg.getJsonMessage());
            // TODO Process it via an Async method
        }, ()->{log.debug("No message yet");});
    }

}
