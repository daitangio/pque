package com.gioorgi.pque.controllers;
import static com.gioorgi.pque.FIXRequest.FixMessageType.QUOTE;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gioorgi.pque.FIXRequest;
import com.gioorgi.pque.client.PGMQClient;
import com.gioorgi.pque.client.PGMQClient.PqueMetric;
import com.gioorgi.pque.client.config.PGMQConfiguration;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LoadTestApi{
    
    @Autowired
    PGMQClient pgmqClient;

    @Autowired
    PGMQConfiguration pgmqConfiguration;

    @GetMapping("/v1/info")
    public ResponseEntity<String> info(){
        var list=pgmqClient.listQueues();
        return  ResponseEntity.ok("Pque ok. Queues:"+list);
    }

    @PostMapping("/v1/send")
    public ResponseEntity<String> send(Object jsonObject){
        pgmqClient.sendObject("market_request", jsonObject);        
        return ResponseEntity.ok("SENT Delay:"+pgmqConfiguration.getDelay());
    }

    @GetMapping("/v1/status")
    public ResponseEntity<List<PqueMetric>> status(){
        return ResponseEntity.ok(pgmqClient.getMetrics());
    }
    @GetMapping("/v1/loadtest/{multiplexer}")
    public ResponseEntity<PqueMetric> load(@PathVariable("multiplexer") Long multiplexer){    
        // Make a load test 
        var request=
            FIXRequest
            .builder()
            .msgType(QUOTE)
            .quoteReqId("123")
            .symbol("EURUSD")
            .quantity(new BigDecimal("230"))
            .settlType("SP")
            .settlDate("20230612")
            .transactTime(LocalDateTime.now())
            .validUntilTime(LocalDateTime.now().plusMinutes(5))
            .dailyFlag("N")
            .exoticFlag("N")
            .build();
        var now=LocalDateTime.now();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHSS");
        String formattedDate = now.format(formatter);    

        for(int i=0; i<=100*multiplexer; i++){
            String finalId=formattedDate+"_"+i;
            request.setQuoteReqId(finalId);
            request.setQuantity(new BigDecimal(i));
            pgmqClient.sendObject("market_request", request);
            if(i%950 == 0){
                log.info("** Loaded {} msg so far",i);
            }
        }

        return ResponseEntity.ok(
            pgmqClient.getMetrics("market_request"));
    }
}
