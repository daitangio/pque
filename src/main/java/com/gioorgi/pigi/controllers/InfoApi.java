package com.gioorgi.pigi.controllers;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.gioorgi.pigi.FIXRequest;
import static com.gioorgi.pigi.FIXRequest.FixMessageType.*;

import io.tembo.pgmq.PGMQClient;
import io.tembo.pgmq.PGMQClient.PigiMetric;
import io.tembo.pgmq.config.PGMQConfiguration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class InfoApi {
    
    @Autowired
    PGMQClient pgmqClient;

    @Autowired
    PGMQConfiguration pgmqConfiguration;

    @GetMapping("/v1/info")
    public ResponseEntity<String> info(){
        var list=pgmqClient.listQueues();
        return  ResponseEntity.ok("PIGI ok. Queues:"+list);
    }

    @PostMapping("/v1/send")
    public ResponseEntity<String> send(Object jsonObject){
        pgmqClient.send("market_request", jsonObject);
        
        return ResponseEntity.ok("SENT Delay:"+pgmqConfiguration.getDelay());
    }

    @GetMapping("/v1/status")
    public ResponseEntity<List<PigiMetric>> status(){
        return ResponseEntity.ok(pgmqClient.getMetrics());
    }
    @GetMapping("/v1/loadtest/{multiplexer}")
    public ResponseEntity<PigiMetric> load(@PathVariable("multiplexer") Long multiplexer){    
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
            pgmqClient.send("market_request", request);
            if(i%950 == 0){
                log.info("** Loaded {} msg so far",i);
            }
        }

        return ResponseEntity.ok(
            pgmqClient.getMetrics("market_request"));
    }
}
