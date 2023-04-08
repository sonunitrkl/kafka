package com.learnkafka.kafkalearning.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.kafkalearning.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Sonu Kumar
 */
@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    final String topic = "Library-events";

    /**
     * sendDefault(..) method is used for sending the message to default topic that is tighted to the configuration value with key as default-topic.
     * this is used to send asynchronously.
     * @param libraryEvent
     * @throws JsonProcessingException
     */
    public void sendLibraryEventAsynchronous(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        final ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = kafkaTemplate.sendDefault(key, value);
        sendResultListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    /**
     *  kafkaTemplate.send(topic,key, value) : this method is used to send message async also.
     *  here we have an option to send the topic also. so by this method we can send message to n number of topic.
     *  Here we have two choices, one is to pass directly topic, key, value in send method as argument.
     *  Another option is to build the producer Record and then directly pass the producer record in the send method.
     * @param libraryEvent
     * @return
     * @throws JsonProcessingException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void sendLibraryEventAsynchronousApproach2(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        try {
            ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key,value,topic);
            //ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture =  kafkaTemplate.send(topic,key, value);
            ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = kafkaTemplate.send(producerRecord);
            sendResultListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
                @Override
                public void onFailure(Throwable ex) {
                    handleFailure(key, value, ex);
                }

                @Override
                public void onSuccess(SendResult<Integer, String> result) {
                    handleSuccess(key, value, result);
                }
            });
        } catch (Exception e) {
            log.error("Exception sending the message and the exception is {} ", e.getMessage());

        }
    }

    /**
     * here header is optional. you can pass some extra information in the header. like, where this event got generated.
     * @param key
     * @param value
     * @param topic
     * @return
     */
    private ProducerRecord<Integer,String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source","scanner".getBytes(StandardCharsets.UTF_8)));
        return new ProducerRecord<>(topic,null,key, value,recordHeaders);
    }

    /**
     * sendDefault(..) method is used for sending the message to default topic that is tighted to the configuration value with key as default-topic.
     * this is used to send synchronously.
     * here, we have get method for sendresult, there we have two option,
     * kafkaTemplate.sendDefault(key, value).get() : this is the overloaded version of get method. it will return only after success or failure
     * kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS); // here we can set our custom timeout feature. if the success or failure is
     * not decided in given time interval, then it will be time out
     * @param libraryEvent
     * @return
     * @throws JsonProcessingException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    // This is sending messages sync
    public SendResult<Integer, String> sendLibraryEventSynchronus(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;
        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS); // here we can set our custom timeout feature. if the success or failure is not decided in given time interval, then it will be time out
            //sendResult = kafkaTemplate.sendDefault(key, value).get(); this is the overloaded version of above get method. it will return only after success or failure
        } catch (ExecutionException | InterruptedException exception) {
            log.error("ExecutionException/InterruptedException sending the message and the exception is {} ", exception.getMessage());
            throw exception;
        } catch (Exception e) {
            log.error("Exception sending the message and the exception is {} ", e.getMessage());

        }
        return sendResult;
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error(" Error sending the message and the exception is  ", ex);
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error on failure :: {} ", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message sent successfully for the key :: {} and the value is :: {} and partition is :: {} ", key, value, result.getRecordMetadata().partition());
    }
}
