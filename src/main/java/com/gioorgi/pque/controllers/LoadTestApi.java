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
import com.gioorgi.pque.client.PQUEClient;
import com.gioorgi.pque.client.PQUEClient.PQUEMetric;
import com.gioorgi.pque.client.config.PQUEConfiguration;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LoadTestApi{
    
    @Autowired
    PQUEClient pqueClient;

    @Autowired
    PQUEConfiguration pqueConfiguration;

    @GetMapping("/v1/info")
    public ResponseEntity<String> info(){
        var list=pqueClient.listQueues();
        return  ResponseEntity.ok("Pque ok. Queues:"+list);
    }

    @PostMapping("/v1/send")
    public ResponseEntity<String> send(Object jsonObject){
        pqueClient.send("market_request", jsonObject);        
        return ResponseEntity.ok("SENT Delay:"+pqueConfiguration.getDelay());
    }

    @GetMapping("/v1/status")
    public ResponseEntity<List<PQUEMetric>> status(){
        return ResponseEntity.ok(pqueClient.getMetrics());
    }
    /**
     * To be truly correct, we should avoid loading messages with the same system we are examining.
     * TODO: Replace this method with a PLSQL procedure
     * @param multiplexer
     * @return
     */
    @GetMapping("/v1/loadtest/{multiplexer}")
    public ResponseEntity<PQUEMetric> load(@PathVariable("multiplexer") Long multiplexer){    
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
        request.setQuantity(new BigDecimal(multiplexer));
        for(int i=0; i<=10*multiplexer; i++){
            String finalId=formattedDate+"_"+i;
            request.setQuoteReqId(finalId);           
            pqueClient.send("market_request", request);
            if(i%950 == 0){
                log.info("** Loaded {} msg so far",i);
            }
        }

        return ResponseEntity.ok(
            pqueClient.getMetrics("market_request"));
    }
}
