package com.gioorgi.pque;

import java.beans.Transient;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.gioorgi.pque.client.PQUEClient;
import com.gioorgi.pque.client.json.PQUEJsonProcessor;

import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Service
@Setter
@Slf4j
@ToString
public class MarketConsumer {

    @Autowired
    PQUEClient pqueClient;

    @Autowired
    PQUEJsonProcessor jsonProcessor;

    @Autowired
    MarketEmulator marketEmulator;

    @Autowired
    @Qualifier("applicationTaskExecutor")
    Executor consumerExcutor;

    static float best_msg_sec=0, worst_msg_sec=10000000000f;

    @Scheduled(fixedDelay = 5, timeUnit = TimeUnit.SECONDS)
    @Transactional
    public void processMarketRequests() {
        log.trace("Checking for message...");
        long processedMessages=0;
        long startTime = System.currentTimeMillis();
        while (true) {
            Optional<FIXRequest> request2Process = pqueClient.pop("market_request",FIXRequest.class);
            if(request2Process.isPresent()){                
                marketEmulator.send2Market(request2Process.get());
                processedMessages++;
                if(processedMessages % 1000 ==0){
                    printStats(processedMessages, startTime);
                }
            }else{
                break;
            }
        }
        if(processedMessages>0){
            printStats(processedMessages, startTime);        
            log.info("No more messages. Executor:{}",this.consumerExcutor);            
        }
    }



    private void printStats(long processedMessages, long startTime) {
        float msg_sec;
        float timeframe = System.currentTimeMillis()-startTime;
        if(timeframe <=0.0 ){
            msg_sec=processedMessages;
        }else {
            msg_sec=processedMessages/(timeframe/1000);
        }
        
        if(msg_sec > best_msg_sec){
            best_msg_sec=msg_sec;
        }
        if(msg_sec < worst_msg_sec){
            worst_msg_sec=msg_sec;
        }
        log.info("Dequeue Best Msg/sec:{} Worst:{} Current:{} Msg so far:{} ",best_msg_sec,worst_msg_sec,msg_sec,processedMessages);
    }

}
