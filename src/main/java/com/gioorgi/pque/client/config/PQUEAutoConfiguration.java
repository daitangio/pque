package com.gioorgi.pque.client.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gioorgi.pque.client.PQUEClient;
import com.gioorgi.pque.client.json.PQUEJsonProcessor;
import com.gioorgi.pque.client.json.PQUEJsonProcessorJackson;

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
@EnableConfigurationProperties(PQUEConfiguration.class)
public class PQUEAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(PQUEJsonProcessor.class)
    @ConditionalOnBean(ObjectMapper.class)
    public PQUEJsonProcessor pqueJsonProcessor(ObjectMapper objectMapper) {
        return new PQUEJsonProcessorJackson(objectMapper);
    }

    @Bean
    @ConditionalOnBean(PQUEJsonProcessor.class)
    public PQUEClient pqueClient(JdbcOperations jdbcOperations,
                                 PQUEConfiguration pqueConfiguration,
                                 PQUEJsonProcessor pqueJsonProcessor) {
        return new PQUEClient(jdbcOperations, pqueConfiguration, pqueJsonProcessor);
    }

}
