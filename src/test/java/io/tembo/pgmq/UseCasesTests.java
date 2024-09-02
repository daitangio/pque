package io.tembo.pgmq;

import io.tembo.pgmq.config.PGMQConfiguration;
import io.tembo.pgmq.config.PGMQVisiblityTimeout;
import io.tembo.pgmq.json.PGMQJsonProcessor;
import lombok.AllArgsConstructor;
import lombok.Getter;

import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = PGMQApplicationTest.class)
@DisplayName("Use cases")
class UseCasesTests {

    @Getter
    @AllArgsConstructor
    public static class Customer {
        String name;
        LocalDate birthday;
        LocalDateTime registeredAt;
        int age;
    }


    @Autowired
    PGMQConfiguration configuration;

    @Autowired
    PGMQClient pgmqClient;

    @Autowired
    PlatformTransactionManager transactionManager;

    @Autowired
    PGMQJsonProcessor jsonProcessor;

    @Nested
    @DisplayName("Read")
    class Read {
        @Test
        @DisplayName("                Read message again if not deleted")
        void readMessageWithoutDelete() throws InterruptedException {
            PGMQueue queue = new PGMQueue("without_delete_queue");
            pgmqClient.createQueue(queue);

            long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

            PGMQMessage message = pgmqClient.read(queue, new PGMQVisiblityTimeout(1)).orElseThrow();
            Assertions.assertEquals(messageId, message.id());

            Thread.sleep(Duration.ofSeconds(2).toMillis()); // sorry for delay

            PGMQMessage sameMessage = pgmqClient.read(queue).orElseThrow();
            Assertions.assertEquals(messageId, sameMessage.id());

            pgmqClient.dropQueue(queue);
        }
    }

    @Nested
    @DisplayName("Delete")
    class Delete {

        @Nested
        @DisplayName("Batch")
        class Batch {
            @Test
            @DisplayName("Success")
            void deleteMessages() {
                PGMQueue queue = new PGMQueue("delete_queue");
                pgmqClient.createQueue(queue);

                List<Long> batchMessages = pgmqClient.sendBatch(queue,
                        List.of(
                                "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }",
                                "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 101 } }"
                        )
                );

                List<Long> messageIds = pgmqClient.deleteBatch(queue, batchMessages);
                assertThat(messageIds).containsExactlyElementsOf(batchMessages);

                pgmqClient.dropQueue(queue);
            }

            @Test
            @DisplayName("Incomplete")
            void failedDeleteMessages() {
                PGMQueue queue = new PGMQueue("delete_queue");
                pgmqClient.createQueue(queue);

                Long messageId = pgmqClient.send(
                        queue,
                        "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }"
                );

                PGMQMessage message = pgmqClient.read(queue).orElseThrow();
                assertThat(message.id()).isEqualTo(messageId);

                List<Long> messageIds = pgmqClient.deleteBatch(queue, List.of(messageId, Long.MAX_VALUE));
                assertThat(messageIds).containsExactly(messageId);

                pgmqClient.dropQueue(queue);
            }
        }

        @Nested
        @DisplayName("Single")
        class Single {
            @Test
            @DisplayName("Success")
            void successDeleteMessage() {
                PGMQueue queue = new PGMQueue("delete_queue");
                pgmqClient.createQueue(queue);

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                PGMQMessage message = pgmqClient.read(queue, new PGMQVisiblityTimeout(1)).orElseThrow();
                Assertions.assertEquals(messageId, message.id());

                boolean deleted = pgmqClient.delete(queue, messageId);
                assertThat(deleted).isTrue();

                pgmqClient.dropQueue(queue);
            }

            @Test
            @DisplayName("Wrong message")
            void failureDeleteMessage() {
                PGMQueue queue = new PGMQueue("delete_queue");
                pgmqClient.createQueue(queue);

                boolean deleted = pgmqClient.delete(queue, Long.MAX_VALUE);
                assertThat(deleted).isFalse();

                pgmqClient.dropQueue(queue);
            }
        }
    }

    @Nested
    @DisplayName("Archive")
    class Archive {

        @Nested
        @DisplayName("Batch")
        class Batch {
            @Test
            @DisplayName("Success")
            void failedBatchMessages() {

                PGMQueue queue = new PGMQueue("delete_queue");
                pgmqClient.createQueue(queue);

                List<Long> batchMessages = pgmqClient.sendBatch(queue,
                        List.of(
                                "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }",
                                "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 101 } }"
                        )
                );

                List<Long> messageIds = pgmqClient.archiveBatch(queue, batchMessages);
                assertThat(messageIds).containsExactlyElementsOf(batchMessages);

                pgmqClient.dropQueue(queue);
            }

            @Test
            @DisplayName("Incomplete")
            void failedIncompleteBatchMessages() {

                PGMQueue queue = new PGMQueue("delete_queue");
                pgmqClient.createQueue(queue);

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                List<Long> messageIds = pgmqClient.archiveBatch(queue, List.of(messageId, Long.MAX_VALUE));
                assertThat(messageIds).containsExactly(messageId);

                pgmqClient.dropQueue(queue);
            }
        }

        @Nested
        @DisplayName("Single")
        class Single {
            @Test
            @DisplayName("Success")
            void singleMessage() {

                PGMQueue queue = new PGMQueue("delete_queue");
                pgmqClient.createQueue(queue);

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                boolean archived = pgmqClient.archive(queue, messageId);
                assertThat(archived).isTrue();

                pgmqClient.dropQueue(queue);
            }

            @Test
            @DisplayName("Wrong message")
            void failedSingleMessage() {

                PGMQueue queue = new PGMQueue("delete_queue");
                pgmqClient.createQueue(queue);

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                boolean archived = pgmqClient.archive(queue, Long.MAX_VALUE);
                assertThat(archived).isFalse();

                pgmqClient.dropQueue(queue);
            }
        }
    }

    @Nested
    @DisplayName("Send")
    class Send {
        @Test
        @DisplayName("                Failed to send message on an invalid queue")
        void sendMessageFailedToInvalidQueue() {
            PGMQueue queue = new PGMQueue("wrong-queue");

            assertThrows(PGMQException.class, () -> pgmqClient.send(queue, "{\"customer_name\": \"John\"}"), "Failed to send message on queue wrong");
        }

        @Test
        @DisplayName("                Failed to send empty message")
        void emptyMessage() {
            PGMQueue queue = new PGMQueue("empty_message");
            pgmqClient.createQueue(queue);

            assertThrows(IllegalArgumentException.class, () -> pgmqClient.send(queue, ""), "Message should be not empty!");

            pgmqClient.dropQueue(queue);
        }

        @Test
        @DisplayName("                Failed to send wrong JSON format message")
        void wrongJsonFormat() {
            PGMQueue queue = new PGMQueue("wrong_json_message");
            pgmqClient.createQueue(queue);

            assertThrows(IllegalArgumentException.class, () -> pgmqClient.send(queue, "{\"customer_name\": \"John}"), "Message should be in JSON format!");

            pgmqClient.dropQueue(queue);
        }

        @Test
        @DisplayName("                Sending multiple messages transactional")
        void sendingTransactional() {
            PGMQueue queue = new PGMQueue("transactional_queue");
            pgmqClient.createQueue(queue);

            TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

            try {
                transactionTemplate.executeWithoutResult((tx) -> {
                    pgmqClient.send(queue, "{\"customer_name\": \"John1\"}");
                    pgmqClient.send(queue, "{\"customer_name\": \"John2\"}");

                    throw new RuntimeException("Something wrong happened");
                });
            } catch (Exception e) {
                // we know it happens
            }

            assertTrue(pgmqClient.readBatch(queue, 2).isEmpty());

            pgmqClient.dropQueue(queue);
        }

        @Test
        @DisplayName("                Sending batch of messages")
        void sendingBatchOfMessages() {
            PGMQueue queue = new PGMQueue("batch_queue");
            pgmqClient.createQueue(queue);

            List<Long> batchMessages = pgmqClient.sendBatch(queue,
                    List.of(
                            "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }",
                            "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 101 } }"
                    )
            );

            List<PGMQMessage> readMessages = pgmqClient.readBatch(queue, 2);
            Assertions.assertEquals(batchMessages.size(), readMessages.size());

            pgmqClient.dropQueue(queue);
        }
    }

    @Nested
    @DisplayName("Pop")
    class PopTests {

        @Test
        @DisplayName("                No message available")
        void popEmptyQueue() {
            PGMQueue queue = new PGMQueue("batch_queue");
            pgmqClient.createQueue(queue);

            assertThat(pgmqClient.pop(queue)).isEmpty();

            pgmqClient.dropQueue(queue);
        }


        // FIXME: Test is not working as expected
        //@Test
        @DisplayName("Success")
        void popFullQueue() {
            PGMQueue queue = new PGMQueue("batch_queue");
            pgmqClient.createQueue(queue);



            Customer customer = new Customer("John", LocalDate.of(1990, 2, 1), LocalDateTime.now(), 34);
            Long messageId = pgmqClient.send(
                    queue,
                    jsonProcessor.toJson(customer)
            );

            PGMQMessage message = pgmqClient.pop(queue).orElseThrow();

            assertThat(message.id()).isEqualTo(messageId);
            assertThat(jsonProcessor.fromJson(message.getJsonMessage(), Customer.class))                    
                    // FIXME: .usingRecursiveAssertion()
                    .isEqualTo(customer);

            pgmqClient.dropQueue(queue);
        }
    }

}
