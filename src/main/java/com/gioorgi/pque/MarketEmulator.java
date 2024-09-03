package com.gioorgi.pque;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.gioorgi.pque.client.PGMQClient;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Service
@Setter
@Slf4j
public class MarketEmulator {

    @Autowired
    PGMQClient pgmqClient;

    public static AtomicLong processed= new AtomicLong();

    ThreadLocal<Random> randomGenerator = ThreadLocal.withInitial(() -> new Random(23));

    @Async
    public void send2Market(FIXRequest request) {
        try {
            // Emulate send to market, wait a bit for processing and then send the response
            // back if the quantity is big, the time increase
            long sleepTime = randomGenerator.get().nextLong(10+ (request.getQuantity().intValue())/2 )+10;
            Thread.sleep(sleepTime);
            String response = "Processed into " + sleepTime + " Req:" + request.getQuoteReqId() + "Qty:"
                    + request.getQuantity();
            // Optionally, send it back
            pgmqClient.send("market_response", response);
            long processedSoFar=processed.incrementAndGet();
            // Show just 5%
            if (randomGenerator.get().nextInt(100) <=5) {
                log.info("{}) Response: {} SleepTime:{}",processedSoFar,response, sleepTime);
            }
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

}