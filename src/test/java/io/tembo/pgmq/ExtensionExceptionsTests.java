package io.tembo.pgmq;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(classes = PGMQApplicationTest.class)
@DisplayName("PGMQ extension exceptions")
class ExtensionExceptionsTests {


    @Autowired
    PGMQClient pgmqClient;

    @Test
    @DisplayName("Extension can't be enabled because it's not installed on bare metal postgres image")
    void extensionNotInstalled() {
        assertThrows(
                PGMQException.class,
                () -> pgmqClient.enableExtension(),
                "Failed to enable 'pgmq' extension"
        );
    }

    @Test
    @DisplayName("No operation is available due to the absence of extension")
    void queueCantBeCreated() {
        var queue = new PGMQueue("baremetal");

        assertThrows(
                PGMQException.class,
                () -> pgmqClient.createQueue(queue),
                "Failed to create queue baremetal"
        );

        assertThrows(
                PGMQException.class,
                () -> pgmqClient.dropQueue(queue),
                "Failed to drop queue baremetal"
        );

        assertThrows(
                PGMQException.class,
                () -> pgmqClient.send(queue, "{\"id\": 1}"),
                "Failed to send message on queue baremetal"
        );

        assertThrows(
                PGMQException.class,
                () -> pgmqClient.read(queue),
                "Failed to read messages from queue baremetal"
        );

        assertThrows(
                PGMQException.class,
                () -> pgmqClient.pop(queue),
                "Failed to pop message from queue baremetal"
        );
    }
}
