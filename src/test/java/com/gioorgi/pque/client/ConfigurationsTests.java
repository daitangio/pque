package com.gioorgi.pque.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.gioorgi.pque.client.config.PQUEConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("Configurations")
class ConfigurationsTests {

    @Nested
    @DisplayName("Delay")
    class DelayTests {
        @Test
        @DisplayName("Default value seconds configuration")
        void defaultDelay() {
            var configuration = new PQUEConfiguration();

            assertThat(configuration.getDelay().getSeconds()).isZero();
        }

        @Test
        @DisplayName("Negative default seconds configuration")
        void negativeDelay() {
            var configuration = new PQUEConfiguration();

            assertThrows(IllegalArgumentException.class,
                    () -> configuration.setDelay(-1),
                    "Delay seconds must be equals or greater than zero!");
        }

        @Test
        @DisplayName("Greater or equal than zero seconds configuration")
        void greaterOrEqualThanZero() {
            var configuration = new PQUEConfiguration();

            configuration.setDelay(0);
            assertThat(configuration.getDelay().getSeconds()).isZero();

            configuration.setDelay(10);
            assertThat(configuration.getDelay().getSeconds()).isEqualTo(10);
        }
    }

    @Nested
    @DisplayName("Visibility timeout")
    class VisibilityTimeoutTests {
        @Test
        @DisplayName("Default visibility timeout (VT) configuration")
        void defaultDelay() {
            var configuration = new PQUEConfiguration();

            assertThat(configuration.getVisibilityTimeout().getSeconds()).isEqualTo(30);
        }

        @Test
        @DisplayName("Negative default visibility timeout (VT) configuration")
        void negativeDelay() {
            var configuration = new PQUEConfiguration();

            assertThrows(IllegalArgumentException.class,
                    () -> configuration.setVisibilityTimeout(-1),
                    "Visibility timeout seconds must be equals or greater than zero!");
        }

        @Test
        @DisplayName("Greater or equal than zero visibility timeout (VT) configuration")
        void greaterOrEqualThanZero() {
            var configuration = new PQUEConfiguration();

            configuration.setVisibilityTimeout(0);
            assertThat(configuration.getVisibilityTimeout().getSeconds()).isZero();

            configuration.setVisibilityTimeout(10);
            assertThat(configuration.getVisibilityTimeout().getSeconds()).isEqualTo(10);
        }
    }
}
