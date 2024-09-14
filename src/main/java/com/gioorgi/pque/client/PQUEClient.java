package com.gioorgi.pque.client;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.gioorgi.pque.client.config.PQUEConfiguration;
import com.gioorgi.pque.client.config.PQUEDelay;
import com.gioorgi.pque.client.config.PQUEVisiblityTimeout;
import com.gioorgi.pque.client.json.PQUEJsonProcessor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
/**
 * Client heavly refactored from https://github.com/adamalexandru4/pgmq-spring
 * It offers high and low level API to pop/push json-serialized messages.
 * It also offer batch and delay functionalities.
 * 
 * @author GG
 */
@Slf4j
public class PQUEClient {

    public static final String QUEUE_MUST_BE_NOT_NULL = "Queue must not be null!";

    @Getter
    @ToString
    @AllArgsConstructor
    public static class PQUEMetric {
        String queueName;
        Long queueLength ;
        int newestMsgAgeSec;
        int oldestMsgAgeSec;
        long totalMessages;
        LocalDateTime scrapeTime;
    }

    private void validateQueueName(String queueName){        
        if (!StringUtils.hasText(queueName)) {
                throw new PQUEException("Name of the queue must not be null with non-empty characters!");
        }
    }

    private final JdbcOperations operations;
    private final PQUEConfiguration configuration;
    private final PQUEJsonProcessor jsonProcessor;

    public PQUEClient(JdbcOperations operations, PQUEConfiguration configuration, PQUEJsonProcessor jsonProcessor) {
        Assert.notNull(operations, "JdbcOperations must not be null!");
        Assert.notNull(configuration, "PGMQConfiguration must not be null!");
        Assert.notNull(jsonProcessor, "PGMQJsonProcessor must not be null!");

        this.operations = operations;
        this.configuration = configuration;
        this.jsonProcessor = jsonProcessor;
    }




    private final long sendWithDelayLowLevel(String queue, String jsonMessage, PQUEDelay delay) {
        validateQueueName(queue);

        if (configuration.isCheckMessage()) {
            Assert.isTrue(StringUtils.hasText(jsonMessage), "Message should not be empty!");
            Assert.isTrue(jsonProcessor.isJson(jsonMessage), "Message should be in JSON format!");
        }

        Long messageId;
        try {
            messageId = operations.queryForObject("select * from pque_send(?, ?::JSONB, ?)", (rs, rn) -> rs.getLong(1), queue, jsonMessage, delay.getSeconds());
            log.trace("Msgid {} Sent with delay {}seconds",messageId,delay.getSeconds());
        } catch (DataAccessException exception) {
            throw new PQUEException("Failed to send message on queue " + queue, exception);
        }

        return Optional.ofNullable(messageId)
                .orElseThrow(() -> new PQUEException("No message id provided for sent message!"));
    }


    public <T extends Object> long sendWithDelay(String queue, T objectMessage, PQUEDelay delay) {
        return sendWithDelayLowLevel(queue, jsonProcessor.toJson(objectMessage), delay);
    }

    /** Send one message with default delay
     * 
     */
    public <T extends Object> long send(String queue, T objectMessage) {
        if (configuration.isCheckMessage()) {
            if (objectMessage instanceof String) {
                Assert.isTrue(StringUtils.hasText((String)objectMessage), "Message should not be empty!");
            }else{
                Assert.notNull(objectMessage,"Message must not be null!");
            }
        }
        return sendWithDelayLowLevel(queue, jsonProcessor.toJson(objectMessage), configuration.getDelay());
    }


    public <T extends Object> List<Long> sendBatchWithDelay(String queue, List<T> objectMessageList, PQUEDelay delay) {
        List<String> jsonMessages=objectMessageList.stream().map(jsonProcessor::toJson).collect(Collectors.toList());
        return sendBatchWithDelayLowLevel(queue, jsonMessages, delay);
    }

    /** Send with default configured delay, in batched
     * 
     */
    public <T extends Object> List<Long> sendBatch(String queue, List<T> jsonMessages) {
        return sendBatchWithDelay(queue, jsonMessages, configuration.getDelay());
    }

    private List<Long> sendBatchWithDelayLowLevel(String queue, List<String> jsonMessages, PQUEDelay delay) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        if (configuration.isCheckMessage()) {
            Assert.isTrue(jsonMessages.stream().allMatch(StringUtils::hasText), "Messages should not be empty!");
            Assert.isTrue(jsonMessages.stream().allMatch(jsonProcessor::isJson), "Messages should be in JSON format!");
        }

        return operations.query("select * from pque_send_batch(?, ?::JSONB[], ?)", (rs, rn) -> rs.getLong(1), queue, jsonMessages.toArray(String[]::new), delay.getSeconds());
    }

    public Optional<PQUEMessage> read(String queue) {
        return read(queue, configuration.getVisibilityTimeout());
    }

    public Optional<PQUEMessage> read(String queue, PQUEVisiblityTimeout visibilityTimeout) {
        return Optional.ofNullable(DataAccessUtils.singleResult(readBatch(queue, visibilityTimeout, 1)));
    }

    public List<PQUEMessage> readBatch(String queue, PQUEVisiblityTimeout visibilityTimeout, int quantity) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);
        Assert.isTrue(quantity > 0, "Number of messages for read must be positive!");

        try {
            return operations.query(
                    "select * from pque_read(?, ?, ?)",
                    (rs, rowNum) -> new PQUEMessage(
                            rs.getLong("msg_id"),
                            rs.getLong("read_ct"),
                            rs.getObject("enqueued_at", OffsetDateTime.class),
                            rs.getObject("vt", OffsetDateTime.class),
                            rs.getString("message")
                    ),
                    queue, visibilityTimeout.getSeconds(), quantity);
        } catch (DataAccessException exception) {
            throw new PQUEException("Failed to read messages from queue " + queue, exception);
        }
    }

    public List<PQUEMessage> readBatch(String queue, int quantity) {
        return readBatch(queue, configuration.getVisibilityTimeout(), quantity);
    }

    /**
     * Pop directly a typed object
     */
    public <T> Optional<T> pop(String queue, Class<T> requiredType){
        Optional<PQUEMessage> msg=popMsg(queue);
        if(msg.isEmpty()){
            return Optional.ofNullable(null);
        }else{
            String json=msg.get().getJsonMessage();
            return Optional.of(jsonProcessor.fromJson(json, requiredType));
        }   
    }
    
    public Optional<PQUEMessage> popMsg(String queue) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        try {
            return Optional.ofNullable(
                    DataAccessUtils.singleResult(
                            operations.query(
                                    "select * from pque_pop(?)",
                                    (rs, rowNum) -> new PQUEMessage(
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
            throw new PQUEException("Failed to pop message from queue " + queue, exception);
        }
    }

    public boolean delete(String queue, long messageId) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        Boolean b = operations.queryForObject("select * from pque_delete(?, ?)", Boolean.class, queue, messageId);

        if (b == null) {
            throw new PQUEException("Error during deletion of message from queue!");
        }

        return b;
    }

    public List<Long> deleteBatch(String queue, List<Long> messageIds) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        List<Long> messageIdsDeleted = operations.query("select * from pque_delete(?, ?)", (rs, rn) -> rs.getLong(1), queue, messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            log.warn("Some messages were not deleted!");
        }

        return messageIdsDeleted;
    }

    public boolean archive(String queue, long messageId) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        Boolean b = operations.queryForObject("select * from pque_archive(?, ?)", Boolean.class, queue, messageId);

        if (b == null) {
            throw new PQUEException("Error during archiving message from queue!");
        }

        return b;
    }

    public List<Long> archiveBatch(String queue, List<Long> messageIds) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        List<Long> messageIdsDeleted = operations.query("select * from pque_archive(?, ?)", (rs, rn) -> rs.getLong(1), queue, messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            log.warn("Some messages were not archived!");
        }

        return messageIdsDeleted;
    }



    public List<String> listQueues() {
        // select * from pque_queue_list();
        List<String> qList=operations.queryForList("select * from pque_queue_list()",String.class);
        return qList;
    }



    public List<PQUEMetric> getMetrics(){
        var metrics=new ArrayList<PQUEMetric>();
        // pque_metrics
        for(var queue : this.listQueues()){
            var m = getMetrics(queue);
            metrics.add(m);
        }
        return metrics;
    }




    public PQUEMetric getMetrics(String queue) {
        PQUEMetric m=operations.queryForObject("select * from pque_metrics(?)",(rs,rn) -> {
            return new PQUEMetric(
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
