package com.gioorgi.pque.client.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gioorgi.pque.client.PGMQClient;
import com.gioorgi.pque.client.json.PGMQJsonProcessor;
import com.gioorgi.pque.client.json.PGMQJsonProcessorJackson;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcOperations;

@AutoConfiguration(after = {
        JacksonAutoConfiguration.class,
        DataSourceAutoConfiguration.class
})
@EnableConfigurationProperties(PGMQConfiguration.class)
public class PGMQAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(PGMQJsonProcessor.class)
    @ConditionalOnBean(ObjectMapper.class)
    public PGMQJsonProcessor pgmqJsonProcessor(ObjectMapper objectMapper) {
        return new PGMQJsonProcessorJackson(objectMapper);
    }

    @Bean
    @ConditionalOnBean(PGMQJsonProcessor.class)
    public PGMQClient pgmqClient(JdbcOperations jdbcOperations,
                                 PGMQConfiguration pgmqConfiguration,
                                 PGMQJsonProcessor pgmqJsonProcessor) {
        return new PGMQClient(jdbcOperations, pgmqConfiguration, pgmqJsonProcessor);
    }

}
