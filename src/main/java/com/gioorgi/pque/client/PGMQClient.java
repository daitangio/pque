package com.gioorgi.pque.client;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.gioorgi.pque.client.config.PGMQConfiguration;
import com.gioorgi.pque.client.config.PGMQDelay;
import com.gioorgi.pque.client.config.PGMQVisiblityTimeout;
import com.gioorgi.pque.client.json.PGMQJsonProcessor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

public class PGMQClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PGMQClient.class);
    public static final String QUEUE_MUST_BE_NOT_NULL = "Queue must be not null!";

    @Getter
    @ToString
    @AllArgsConstructor
    public static class PqueMetric {
        String queueName;
        Long queueLength ;
        int newestMsgAgeSec;
        int oldestMsgAgeSec;
        long totalMessages;
        LocalDateTime scrapeTime;
    }

    private void validateQueueName(String queueName){        
        if (!StringUtils.hasText(queueName)) {
                throw new PGMQException("Name of the queue must be not null with non-empty characters!");
        }
    }

    private final JdbcOperations operations;
    private final PGMQConfiguration configuration;
    private final PGMQJsonProcessor jsonProcessor;

    public PGMQClient(JdbcOperations operations, PGMQConfiguration configuration, PGMQJsonProcessor jsonProcessor) {
        Assert.notNull(operations, "JdbcOperations must be not null!");
        Assert.notNull(configuration, "PGMQConfiguration must be not null!");
        Assert.notNull(jsonProcessor, "PGMQJsonProcessor must be not null!");

        this.operations = operations;
        this.configuration = configuration;
        this.jsonProcessor = jsonProcessor;
    }




    public long sendWithDelay(String queue, String jsonMessage, PGMQDelay delay) {
        validateQueueName(queue);

        if (configuration.isCheckMessage()) {
            Assert.isTrue(StringUtils.hasText(jsonMessage), "Message should be not empty!");
            Assert.isTrue(jsonProcessor.isJson(jsonMessage), "Message should be in JSON format!");
        }

        Long messageId;
        try {
            messageId = operations.queryForObject("select * from pque_send(?, ?::JSONB, ?)", (rs, rn) -> rs.getLong(1), queue, jsonMessage, delay.getSeconds());
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to send message on queue " + queue, exception);
        }

        return Optional.ofNullable(messageId)
                .orElseThrow(() -> new PGMQException("No message id provided for sent message!"));
    }


    public long send(String queue, String jsonMessage) {
        return sendWithDelay(queue, jsonMessage, configuration.getDelay());
    }


    public long sendObject(String queue, Object msgObj) {
        return this.send(queue, jsonProcessor.toJson(msgObj));
    }





    public List<Long> sendBatchWithDelay(String queue, List<String> jsonMessages, PGMQDelay delay) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        if (configuration.isCheckMessage()) {
            Assert.isTrue(jsonMessages.stream().allMatch(StringUtils::hasText), "Messages should be not empty!");
            Assert.isTrue(jsonMessages.stream().allMatch(jsonProcessor::isJson), "Messages should be in JSON format!");
        }

        return operations.query("select * from pque_send_batch(?, ?::JSONB[], ?)", (rs, rn) -> rs.getLong(1), queue, jsonMessages.toArray(String[]::new), delay.getSeconds());
    }


    public List<Long> sendBatch(String queue, List<String> jsonMessages) {
        return sendBatchWithDelay(queue, jsonMessages, configuration.getDelay());
    }

    // /** FIXME: Cover with test */
    // public List<Long> sendBatch(String queue, List<Object> objects) {
    //     var jsonized=objects.stream().map(jsonProcessor::toJson).toList();
    //     return sendBatch(queue, jsonized);
    // }

    public Optional<PGMQMessage> read(String queue) {
        return read(queue, configuration.getVisibilityTimeout());
    }

    public Optional<PGMQMessage> read(String queue, PGMQVisiblityTimeout visibilityTimeout) {
        return Optional.ofNullable(DataAccessUtils.singleResult(readBatch(queue, visibilityTimeout, 1)));
    }

    public List<PGMQMessage> readBatch(String queue, PGMQVisiblityTimeout visibilityTimeout, int quantity) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);
        Assert.isTrue(quantity > 0, "Number of messages for read must be positive!");

        try {
            return operations.query(
                    "select * from pque_read(?, ?, ?)",
                    (rs, rowNum) -> new PGMQMessage(
                            rs.getLong("msg_id"),
                            rs.getLong("read_ct"),
                            rs.getObject("enqueued_at", OffsetDateTime.class),
                            rs.getObject("vt", OffsetDateTime.class),
                            rs.getString("message")
                    ),
                    queue, visibilityTimeout.getSeconds(), quantity);
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to read messages from queue " + queue, exception);
        }
    }

    public List<PGMQMessage> readBatch(String queue, int quantity) {
        return readBatch(queue, configuration.getVisibilityTimeout(), quantity);
    }

    public Optional<PGMQMessage> pop(String queue) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        try {
            return Optional.ofNullable(
                    DataAccessUtils.singleResult(
                            operations.query(
                                    "select * from pque_pop(?)",
                                    (rs, rowNum) -> new PGMQMessage(
                                            rs.getLong("msg_id"),
                                            rs.getLong("read_ct"),
                                            rs.getObject("enqueued_at", OffsetDateTime.class),
                                            rs.getObject("vt", OffsetDateTime.class),
                                            rs.getString("message")
                                    ),
                                    queue)
                    )
            );
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to pop message from queue " + queue, exception);
        }
    }

    public boolean delete(String queue, long messageId) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        Boolean b = operations.queryForObject("select * from pque_delete(?, ?)", Boolean.class, queue, messageId);

        if (b == null) {
            throw new PGMQException("Error during deletion of message from queue!");
        }

        return b;
    }

    public List<Long> deleteBatch(String queue, List<Long> messageIds) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        List<Long> messageIdsDeleted = operations.query("select * from pque_delete(?, ?)", (rs, rn) -> rs.getLong(1), queue, messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            LOGGER.warn("Some messages were not deleted!");
        }

        return messageIdsDeleted;
    }

    public boolean archive(String queue, long messageId) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        Boolean b = operations.queryForObject("select * from pque_archive(?, ?)", Boolean.class, queue, messageId);

        if (b == null) {
            throw new PGMQException("Error during archiving message from queue!");
        }

        return b;
    }

    public List<Long> archiveBatch(String queue, List<Long> messageIds) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        List<Long> messageIdsDeleted = operations.query("select * from pque_archive(?, ?)", (rs, rn) -> rs.getLong(1), queue, messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            LOGGER.warn("Some messages were not archived!");
        }

        return messageIdsDeleted;
    }




    public List<String> listQueues() {
        // select * from pque_queue_list();
        List<String> qList=operations.queryForList("select * from pque_queue_list()",String.class);
        return qList;
    }



    public List<PqueMetric> getMetrics(){
        var metrics=new ArrayList<PqueMetric>();
        // pque_metrics
        for(var queue : this.listQueues()){
            var m = getMetrics(queue);
            metrics.add(m);
        }
        return metrics;
    }




    public PqueMetric getMetrics(String queue) {
        PqueMetric m=operations.queryForObject("select * from pque_metrics(?)",(rs,rn) -> {
            return new PqueMetric(
                rs.getString("queue_name"),
                rs.getLong("queue_length"),
                rs.getInt("newest_msg_age_sec"),
                rs.getInt("oldest_msg_age_sec"),
                rs.getLong("total_messages"),
                rs.getTimestamp("scrape_time").toLocalDateTime()
            );
        }, queue);
        return m;
    }

}
