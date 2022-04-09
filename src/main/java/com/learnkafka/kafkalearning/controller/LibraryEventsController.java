package com.learnkafka.kafkalearning.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.kafkalearning.domain.LibraryEvent;
import com.learnkafka.kafkalearning.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

/**
 * @author Sonu Kumar
 */
@RestController
@Slf4j
@RequestMapping("/book/v1")
public class LibraryEventsController {

    @Autowired
    private LibraryEventProducer libraryEventProducer;

    @RequestMapping(value = "/libraryupdate",method = RequestMethod.POST)
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
       log.info("Before the Send Library event");
       // libraryEventProducer.sendLibraryEvent(libraryEvent);
        SendResult<Integer, String> stringSendResult = libraryEventProducer.sendLibraryEventSynchronus(libraryEvent);
        log.info("sendResult is {} ", stringSendResult.toString());
        log.info("After the Send Library event");
        return new ResponseEntity<>(libraryEvent, HttpStatus.CREATED);
    }
}
