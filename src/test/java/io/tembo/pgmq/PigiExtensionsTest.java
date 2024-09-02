package io.tembo.pgmq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;

import lombok.extern.slf4j.Slf4j;

@SpringBootTest(classes = PGMQApplicationTest.class)
@Sql("classpath:test-queue.sql")
@Slf4j
public class PigiExtensionsTest {

    @Autowired
    PGMQClient pgmqClient;

    // var list=pgmqClient.listQueues();
    @Test
    public void listQueueWorks(){

        log.info("{}", pgmqClient.listQueues());
        assertEquals(7+2 /* test+demo */,
            pgmqClient.listQueues().size());

    }
}
