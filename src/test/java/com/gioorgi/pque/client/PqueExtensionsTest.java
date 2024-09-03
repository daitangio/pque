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
    PGMQClient pgmqClient;

    // var list=pgmqClient.listQueues();
    @Test
    public void listQueueWorks(){

        log.info("{}", pgmqClient.listQueues());
        assertEquals(7+2 /* test+demo */,
            pgmqClient.listQueues().size());
    }

    @Test 
    public void getOne_Metric(){
        var metrics=pgmqClient.getMetrics("empty_queue");
        log.info("{}",metrics);
        assertEquals(0,metrics.getQueueLength());
        
    }


    @Test 
    public void getMetrics(){
        for(var metric : pgmqClient.getMetrics()){
            log.info("{}",metric);
        }
    }
}
