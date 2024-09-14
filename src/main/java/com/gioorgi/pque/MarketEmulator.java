package com.gioorgi.pque;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.gioorgi.pque.client.PQUEClient;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Service
@Setter
@Slf4j
public class MarketEmulator {

    @Autowired
    PQUEClient pqueClient;

    public static AtomicLong processed= new AtomicLong();

    ThreadLocal<Random> randomGenerator = ThreadLocal.withInitial(() -> new Random(23));

    @Async
    public void send2Market(FIXRequest request) {
        try {
            // Emulate send to market, wait a bit for processing, between 0 and 500ms
            long sleepTime = randomGenerator.get().nextInt(5)*100;
            Thread.sleep(sleepTime);
            String response = "Processed into " + sleepTime + " Req:" + request.getQuoteReqId() + "Qty:"
                    + request.getQuantity();
            // Optionally, send it back, to minimally stress the database
            pqueClient.send("market_response", response);
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
