package com.gioorgi.pque.client.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pque")
public class PQUEConfiguration {

    private PQUEDelay delay = new PQUEDelay(0);

    private PQUEVisiblityTimeout visibilityTimeout = new PQUEVisiblityTimeout(30);

    private boolean checkMessage = true;

    public PQUEDelay getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = new PQUEDelay(delay);
    }

    public PQUEVisiblityTimeout getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public void setVisibilityTimeout(int visibilityTimeout) {
        this.visibilityTimeout = new PQUEVisiblityTimeout(visibilityTimeout);
    }

    public boolean isCheckMessage() {
        return checkMessage;
    }

    public void setCheckMessage(boolean checkMessage) {
        this.checkMessage = checkMessage;
    }
}
