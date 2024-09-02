package io.tembo.pgmq;

import java.time.OffsetDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PGMQMessage {
        Long id;
        Long readCounter;
        OffsetDateTime enqueuedAt;
        OffsetDateTime visibilityTime;
        String jsonMessage;
}
