package com.gioorgi.pigi.controllers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import io.tembo.pgmq.PGMQClient;
import io.tembo.pgmq.config.PGMQConfiguration;
import lombok.Getter;

@RestController
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
}
