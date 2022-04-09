package com.learnkafka.kafkalearning.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.kafkalearning.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

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

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
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

    public SendResult<Integer, String> sendLibraryEventSynchronus(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;
        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException exception) {
            log.error("ExecutionException/InteruptedException sending the message and the exception is {} ", exception.getMessage());
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
        log.info("Message sent successfully for the key :: {} and the value is :: {} and partition is :: {} ", key, value, result.getRecordMetadata());
    }
}
