package com.gioorgi.pque.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;

import lombok.extern.slf4j.Slf4j;

@SpringBootTest(classes = PGMQApplicationTest.class)
@Sql("classpath:test-queue.sql")
@Slf4j
public class PqueExtensionsTest {

    @Autowired
    PQUEClient pqueClient;

    @Test
    public void listQueueWorks(){

        log.info("{}", pqueClient.listQueues());
        assertEquals(7+2 /* test+demo */,
            pqueClient.listQueues().size());
    }

    @Test 
    public void getOne_Metric(){
        var metrics=pqueClient.getMetrics("empty_queue");
        log.info("{}",metrics);
        assertEquals(0,metrics.getQueueLength());
        
    }


    @Test 
    public void getMetrics(){
        for(var metric : pqueClient.getMetrics()){
            log.info("{}",metric);
        }
    }
}
