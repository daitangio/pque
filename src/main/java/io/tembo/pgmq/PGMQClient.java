package io.tembo.pgmq;

import io.tembo.pgmq.config.PGMQConfiguration;
import io.tembo.pgmq.config.PGMQDelay;
import io.tembo.pgmq.config.PGMQVisiblityTimeout;
import io.tembo.pgmq.json.PGMQJsonProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

public class PGMQClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PGMQClient.class);
    public static final String QUEUE_MUST_BE_NOT_NULL = "Queue must be not null!";

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


    public void createQueue(PGMQueue queue) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        try {
            operations.execute("SELECT pigi_create('" + queue.getName() + "')");
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to create queue " + queue.getName(), exception);
        }
    }

    public void dropQueue(PGMQueue queue) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        try {
            operations.execute("SELECT pigi_drop_queue('" + queue.getName() + "')");
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to drop queue " + queue.getName(), exception);
        }
    }

    public long sendWithDelay(PGMQueue queue, String jsonMessage, PGMQDelay delay) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        if (configuration.isCheckMessage()) {
            Assert.isTrue(StringUtils.hasText(jsonMessage), "Message should be not empty!");
            Assert.isTrue(jsonProcessor.isJson(jsonMessage), "Message should be in JSON format!");
        }

        Long messageId;
        try {
            messageId = operations.queryForObject("select * from pigi_send(?, ?::JSONB, ?)", (rs, rn) -> rs.getLong(1), queue.getName(), jsonMessage, delay.getSeconds());
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to send message on queue " + queue.getName(), exception);
        }

        return Optional.ofNullable(messageId)
                .orElseThrow(() -> new PGMQException("No message id provided for sent message!"));
    }


    public long send(PGMQueue queue, String jsonMessage) {
        return sendWithDelay(queue, jsonMessage, configuration.getDelay());
    }

    public List<Long> sendBatchWithDelay(PGMQueue queue, List<String> jsonMessages, PGMQDelay delay) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        if (configuration.isCheckMessage()) {
            Assert.isTrue(jsonMessages.stream().allMatch(StringUtils::hasText), "Messages should be not empty!");
            Assert.isTrue(jsonMessages.stream().allMatch(jsonProcessor::isJson), "Messages should be in JSON format!");
        }

        return operations.query("select * from pigi_send_batch(?, ?::JSONB[], ?)", (rs, rn) -> rs.getLong(1), queue.getName(), jsonMessages.toArray(String[]::new), delay.getSeconds());
    }

    public List<Long> sendBatch(PGMQueue queue, List<String> jsonMessages) {
        return sendBatchWithDelay(queue, jsonMessages, configuration.getDelay());
    }

    public Optional<PGMQMessage> read(PGMQueue queue) {
        return read(queue, configuration.getVisibilityTimeout());
    }

    public Optional<PGMQMessage> read(PGMQueue queue, PGMQVisiblityTimeout visibilityTimeout) {
        return Optional.ofNullable(DataAccessUtils.singleResult(readBatch(queue, visibilityTimeout, 1)));
    }

    public List<PGMQMessage> readBatch(PGMQueue queue, PGMQVisiblityTimeout visibilityTimeout, int quantity) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);
        Assert.isTrue(quantity > 0, "Number of messages for read must be positive!");

        try {
            return operations.query(
                    "select * from pigi_read(?, ?, ?)",
                    (rs, rowNum) -> new PGMQMessage(
                            rs.getLong("msg_id"),
                            rs.getLong("read_ct"),
                            rs.getObject("enqueued_at", OffsetDateTime.class),
                            rs.getObject("vt", OffsetDateTime.class),
                            rs.getString("message")
                    ),
                    queue.getName(), visibilityTimeout.getSeconds(), quantity);
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to read messages from queue " + queue.getName(), exception);
        }
    }

    public List<PGMQMessage> readBatch(PGMQueue queue, int quantity) {
        return readBatch(queue, configuration.getVisibilityTimeout(), quantity);
    }

    public Optional<PGMQMessage> pop(PGMQueue queue) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        try {
            return Optional.ofNullable(
                    DataAccessUtils.singleResult(
                            operations.query(
                                    "select * from pigi_pop(?)",
                                    (rs, rowNum) -> new PGMQMessage(
                                            rs.getLong("msg_id"),
                                            rs.getLong("read_ct"),
                                            rs.getObject("enqueued_at", OffsetDateTime.class),
                                            rs.getObject("vt", OffsetDateTime.class),
                                            rs.getString("message")
                                    ),
                                    queue.getName())
                    )
            );
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to pop message from queue " + queue.getName(), exception);
        }
    }

    public boolean delete(PGMQueue queue, long messageId) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        Boolean b = operations.queryForObject("select * from pigi_delete(?, ?)", Boolean.class, queue.getName(), messageId);

        if (b == null) {
            throw new PGMQException("Error during deletion of message from queue!");
        }

        return b;
    }

    public List<Long> deleteBatch(PGMQueue queue, List<Long> messageIds) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        List<Long> messageIdsDeleted = operations.query("select * from pigi_delete(?, ?)", (rs, rn) -> rs.getLong(1), queue.getName(), messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            LOGGER.warn("Some messages were not deleted!");
        }

        return messageIdsDeleted;
    }

    public boolean archive(PGMQueue queue, long messageId) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        Boolean b = operations.queryForObject("select * from pigi_archive(?, ?)", Boolean.class, queue.getName(), messageId);

        if (b == null) {
            throw new PGMQException("Error during archiving message from queue!");
        }

        return b;
    }

    public List<Long> archiveBatch(PGMQueue queue, List<Long> messageIds) {
        Assert.notNull(queue, QUEUE_MUST_BE_NOT_NULL);

        List<Long> messageIdsDeleted = operations.query("select * from pigi_archive(?, ?)", (rs, rn) -> rs.getLong(1), queue.getName(), messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            LOGGER.warn("Some messages were not archived!");
        }

        return messageIdsDeleted;
    }


}
