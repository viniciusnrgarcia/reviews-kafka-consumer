/**
 *
 */
package com.reviews.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.reviews.domain.Review;

/**
 * @author vnrg
 *
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfiguration {

    /**
     * Servidor kafka
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private transient String bootstrapServers;

    @Bean
    public ConsumerFactory<String, Review> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-reviews");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // How many records a call to poll() will return in a single call.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5_000);
        // props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 3000000);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(Review.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Review> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Review> factory = new ConcurrentKafkaListenerContainerFactory<String, Review>();
        factory.setConsumerFactory(this.consumerFactory());
        // Define o commit log do offset para manual.
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE);
        // factory.setBatchListener(true); // Permite o processamento Batch e passa a
        // receber uma coleção com mensagens.
        // factory.setConcurrency(4); // Concorrência de threads
        return factory;
    }

    @Bean
    public KafkaConsumerListener listener() {
        return new KafkaConsumerListener();
    }

}
