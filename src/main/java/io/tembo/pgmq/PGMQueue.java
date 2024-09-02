package io.tembo.pgmq;

import org.springframework.util.StringUtils;

import lombok.Getter;

@Getter
public class PGMQueue {
    
    String name;

    public PGMQueue(String name) {
        if (!StringUtils.hasText(name)) {
            throw new PGMQException("Name of the queue must be not null with non-empty characters!");
        }
        this.name=name;
    }
}
