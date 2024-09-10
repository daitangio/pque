package com.gioorgi.pque.client;

import java.time.OffsetDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
@Getter()
@AllArgsConstructor
public class PQUEMessage {
        Long id;
        Long readCounter;
        OffsetDateTime enqueuedAt;
        OffsetDateTime visibilityTime;
        String jsonMessage;

        public Long id() { return this.id;}
}
