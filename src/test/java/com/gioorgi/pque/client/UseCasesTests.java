package com.gioorgi.pque.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.gioorgi.pque.client.config.PGMQConfiguration;
import com.gioorgi.pque.client.config.PGMQVisiblityTimeout;
import com.gioorgi.pque.client.json.PGMQJsonProcessor;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = PGMQApplicationTest.class)
@DisplayName("Use cases")
@Sql("classpath:test-queue.sql")
class UseCasesTests {

    
    @AllArgsConstructor
    @Data
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
        @DisplayName("Read message again if not deleted")
        void readMessageWithoutDelete() throws InterruptedException {
            PGMQueue queue = new PGMQueue("without_delete_queue");

            long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

            PGMQMessage message = pgmqClient.read(queue, new PGMQVisiblityTimeout(1)).orElseThrow();
            Assertions.assertEquals(messageId, message.id());

            Thread.sleep(Duration.ofSeconds(2).toMillis()); // sorry for delay

            PGMQMessage sameMessage = pgmqClient.read(queue).orElseThrow();
            Assertions.assertEquals(messageId, sameMessage.id());

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
                List<Long> batchMessages = pgmqClient.sendBatch(queue,
                        List.of(
                                "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }",
                                "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 101 } }"
                        )
                );

                List<Long> messageIds = pgmqClient.deleteBatch(queue, batchMessages);
                assertThat(messageIds).containsExactlyElementsOf(batchMessages);

            }

            @Test
            @DisplayName("Incomplete")
            void failedDeleteMessages() {
                PGMQueue queue = new PGMQueue("delete_queue");
                
                Long messageId = pgmqClient.send(
                        queue,
                        "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }"
                );

                PGMQMessage message = pgmqClient.read(queue).orElseThrow();
                assertThat(message.id()).isEqualTo(messageId);

                List<Long> messageIds = pgmqClient.deleteBatch(queue, List.of(messageId, Long.MAX_VALUE));
                assertThat(messageIds).containsExactly(messageId);

            }
        }

        @Nested
        @DisplayName("Single")
        class Single {
            @Test
            @DisplayName("Success")
            void successDeleteMessage() {
                PGMQueue queue = new PGMQueue("delete_queue");
                

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                PGMQMessage message = pgmqClient.read(queue, new PGMQVisiblityTimeout(1)).orElseThrow();
                Assertions.assertEquals(messageId, message.id());

                boolean deleted = pgmqClient.delete(queue, messageId);
                assertThat(deleted).isTrue();

            }

            @Test
            @DisplayName("Wrong message")
            void failureDeleteMessage() {
                PGMQueue queue = new PGMQueue("delete_queue");
                

                boolean deleted = pgmqClient.delete(queue, Long.MAX_VALUE);
                assertThat(deleted).isFalse();


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
                
                List<Long> batchMessages = pgmqClient.sendBatch(queue,
                        List.of(
                                "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }",
                                "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 101 } }"
                        )
                );

                List<Long> messageIds = pgmqClient.archiveBatch(queue, batchMessages);
                assertThat(messageIds).containsExactlyElementsOf(batchMessages);


            }

            @Test
            @DisplayName("Incomplete")
            void failedIncompleteBatchMessages() {

                PGMQueue queue = new PGMQueue("delete_queue");
                

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                List<Long> messageIds = pgmqClient.archiveBatch(queue, List.of(messageId, Long.MAX_VALUE));
                assertThat(messageIds).containsExactly(messageId);


            }
        }

        @Nested
        @DisplayName("Single")
        class Single {
            @Test
            @DisplayName("Success")
            void singleMessage() {

                PGMQueue queue = new PGMQueue("delete_queue");
                

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                boolean archived = pgmqClient.archive(queue, messageId);
                assertThat(archived).isTrue();


            }

            @Test
            @DisplayName("Wrong message")
            void failedSingleMessage() {

                PGMQueue queue = new PGMQueue("delete_queue");
                

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                boolean archived = pgmqClient.archive(queue, Long.MAX_VALUE);
                assertThat(archived).isFalse();


            }
        }
    }

    @Nested
    @DisplayName("Send")
    class Send {
        @Test
        @DisplayName("Failed to send message on an invalid queue")
        void sendMessageFailedToInvalidQueue() {
            PGMQueue queue = new PGMQueue("wrong-queue");

            assertThrows(PGMQException.class, () -> pgmqClient.send(queue, "{\"customer_name\": \"John\"}"), "Failed to send message on queue wrong");
        }

        @Test
        @DisplayName("Failed to send empty message")
        void emptyMessage() {
            PGMQueue queue = new PGMQueue("empty_message");
            
            assertThrows(IllegalArgumentException.class, () -> pgmqClient.send(queue, ""), "Message should be not empty!");

        }

        @Test
        @DisplayName("Failed to send wrong JSON format message")
        void wrongJsonFormat() {
            PGMQueue queue = new PGMQueue("wrong_json_message");            
            assertThrows(IllegalArgumentException.class, () -> pgmqClient.send(queue, "{\"customer_name\": \"John}"), "Message should be in JSON format!");
        }

        @Test
        @DisplayName("Sending multiple messages transactional")
        void sendingTransactional() {
            PGMQueue queue = new PGMQueue("transactional_queue");
            
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


        }

        @Test
        @DisplayName("Sending batch of messages")
        void sendingBatchOfMessages() {
            PGMQueue queue = new PGMQueue("batch_queue");
            

            List<Long> batchMessages = pgmqClient.sendBatch(queue,
                    List.of(
                            "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }",
                            "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 101 } }"
                    )
            );

            List<PGMQMessage> readMessages = pgmqClient.readBatch(queue, 2);
            Assertions.assertEquals(batchMessages.size(), readMessages.size());


        }
    }

    @Nested
    @DisplayName("Pop")
    class PopTests {

        @Test
        @DisplayName("No message available")
        void popEmptyQueue() {
            PGMQueue queue = new PGMQueue("empty_queue");            
            assertThat(pgmqClient.pop(queue)).isEmpty();
        }



        @Test
        @DisplayName("Success")
        void popFullQueue() {
            PGMQueue queue = new PGMQueue("batch_queue");
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

        }
    }

}
