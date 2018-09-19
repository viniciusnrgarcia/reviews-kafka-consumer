package com.reviews.config;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Vinicius Garcia
 *
 */
@Slf4j
public class KafkaConsumerListener {

    /**
     * Topic
     */
    private static final String TOPIC_NAME = "review-topic";

    public CountDownLatch countDownLatch = new CountDownLatch(3);

    /**
     * @param record
     *            Mensagem
     * @param acknowledgment
     *            {@link Acknowledgment} Ack de callback após processamento da
     *            mensagem.
     */
    @KafkaListener(id = "id0", groupId = "group-reviews", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME, partitions = { "0" }) })
    public void listenPartition0(ConsumerRecord<?, ?> record, final Acknowledgment acknowledgment) {

        log.info("id0 - Listener partition: " + record.partition() + ", Thread ID: " + Thread.currentThread().getId()
                + " Received: " + record + " Partition: " + record.partition() + " Topic: " + record.topic());

        this.countDown(this.countDownLatch, record, acknowledgment);
    }
    
    @KafkaListener(id = "id4", groupId = "group-reviews", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME, partitions = { "0" }) })
    public void listenPartition04(ConsumerRecord<?, ?> record, final Acknowledgment acknowledgment) {

        log.info("id4 - Listener partition: " + record.partition() + ", Thread ID: " + Thread.currentThread().getId()
                + " Received: " + record + " Partition: " + record.partition() + " Topic: " + record.topic());

        this.countDown(this.countDownLatch, record, acknowledgment);
    }

    @KafkaListener(id = "id1", groupId = "group-reviews", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME, partitions = { "1" }) })
    public void listenPartition1(ConsumerRecord<?, ?> record, final Acknowledgment acknowledgment) {

        log.info("id1 - Listener partition: " + record.partition() + ", Thread ID: " + Thread.currentThread().getId()
                + " Received: " + record + " Partition: " + record.partition() + " Topic: " + record.topic());

        this.countDown(this.countDownLatch, record, acknowledgment);
    }

    @KafkaListener(id = "id2", groupId = "group-reviews", topicPartitions = {
            @TopicPartition(topic = TOPIC_NAME, partitions = { "2" }) })
    public void listenPartition2(ConsumerRecord<?, ?> record, final Acknowledgment acknowledgment) {

        log.info("id2 - Listener partition: " + record.partition() + ", Thread ID: " + Thread.currentThread().getId()
                + " Received: " + record + " Partition: " + record.partition() + " Topic: " + record.topic());

        this.countDown(this.countDownLatch, record, acknowledgment);
    }

    /**
     * Implementa o método {@code countDown}.
     * 
     * @param countDownLatch
     *            {@link CountDownLatch}
     * @param record
     *            {@link ConsumerRecord}
     * @param acknowledgment
     */
    private void countDown(final CountDownLatch countDownLatch, final ConsumerRecord<?, ?> record,
            final Acknowledgment acknowledgment) {
        countDownLatch.countDown();
        acknowledgment.acknowledge();
    }

}
