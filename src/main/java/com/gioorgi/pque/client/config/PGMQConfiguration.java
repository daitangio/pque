package com.gioorgi.pque.client.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pgmq")
public class PGMQConfiguration {

    private PGMQDelay delay = new PGMQDelay(0);

    private PGMQVisiblityTimeout visibilityTimeout = new PGMQVisiblityTimeout(30);

    private boolean checkMessage = true;

    public PGMQDelay getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = new PGMQDelay(delay);
    }

    public PGMQVisiblityTimeout getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public void setVisibilityTimeout(int visibilityTimeout) {
        this.visibilityTimeout = new PGMQVisiblityTimeout(visibilityTimeout);
    }

    public boolean isCheckMessage() {
        return checkMessage;
    }

    public void setCheckMessage(boolean checkMessage) {
        this.checkMessage = checkMessage;
    }
}
