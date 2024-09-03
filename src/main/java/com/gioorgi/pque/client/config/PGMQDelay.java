package com.gioorgi.pque.client.config;

import org.springframework.util.Assert;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class PGMQDelay {
    int seconds;
    public PGMQDelay(int s){
        if(s <0 ) {
            throw new IllegalArgumentException("Delay must be >0");
        }
        this.seconds=s;
    }
}
