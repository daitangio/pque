package com.gioorgi.pque.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.gioorgi.pque.client.config.PGMQConfiguration;
import com.gioorgi.pque.client.config.PGMQVisiblityTimeout;
import com.gioorgi.pque.client.json.PGMQJsonProcessor;

import lombok.AllArgsConstructor;
import lombok.Data;

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
            final String queue="without_delete_queue";

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
                final String queue="delete_queue";
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
                final String queue="delete_queue";
                
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
                final String queue="delete_queue";
                

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                PGMQMessage message = pgmqClient.read(queue, new PGMQVisiblityTimeout(1)).orElseThrow();
                Assertions.assertEquals(messageId, message.id());

                boolean deleted = pgmqClient.delete(queue, messageId);
                assertThat(deleted).isTrue();

            }

            @Test
            @DisplayName("Wrong message")
            void failureDeleteMessage() {
                final String queue="delete_queue";
                

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

                final String queue="delete_queue";
                
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

                final String queue="delete_queue";
                

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

                final String queue="delete_queue";
                

                long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

                boolean archived = pgmqClient.archive(queue, messageId);
                assertThat(archived).isTrue();


            }

            @Test
            @DisplayName("Wrong message")
            void failedSingleMessage() {
                final String queue="delete_queue";            
                pgmqClient.send(queue, "{\"customer_name\": \"John\"}");
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
            final String queue="wrong-queue";

            assertThrows(PGMQException.class, () -> pgmqClient.send(queue, "{\"customer_name\": \"John\"}"), "Failed to send message on queue wrong");
        }

        @Test
        @DisplayName("Failed to send empty message")
        void emptyMessage() {
            final String queue="empty_message";
            
            assertThrows(IllegalArgumentException.class, () -> pgmqClient.send(queue, ""), "Message should be not empty!");

        }


        @Test
        @DisplayName("Sending multiple messages transactional")
        void sendingTransactional() {
            final String queue="transactional_queue";
            
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
            final String queue="batch_queue";
            

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
            final String queue="empty_queue";            
            assertThat(pgmqClient.pop(queue,Object.class)).isEmpty();
        }



        @Test
        @DisplayName("Success")
        void popFullQueue() {
            final String queue="batch_queue";
            Customer customer = new Customer("John", LocalDate.of(1990, 2, 1), LocalDateTime.now(), 34);
            Long messageId = pgmqClient.send(
                    queue,
                    customer
            );

            PGMQMessage message = pgmqClient.popMsg(queue).orElseThrow();

            assertThat(message.id()).isEqualTo(messageId);
            assertThat(jsonProcessor.fromJson(message.getJsonMessage(), Customer.class))                                        
                    .isEqualTo(customer);

        }
    }

}
