package com.learnkafka.kafkalearning.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.kafkalearning.domain.LibraryEvent;
import com.learnkafka.kafkalearning.enums.LibraryEventType;
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

    @RequestMapping(value = "/async/libraryupdate",method = RequestMethod.POST)
    public ResponseEntity<LibraryEvent> postLibraryEventAsync(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
       log.info("Before the Send Library event");
        libraryEventProducer.sendLibraryEventAsynchronous(libraryEvent);
        log.info("After the Send Library event");
        return new ResponseEntity<>(libraryEvent, HttpStatus.CREATED);
    }


    @RequestMapping(value = "/sync/libraryupdate",method = RequestMethod.POST)
    public ResponseEntity<LibraryEvent> postLibraryEventSync(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        log.info("Before the Send Library event");
        SendResult<Integer, String> stringSendResult = libraryEventProducer.sendLibraryEventSynchronus(libraryEvent);
        log.info("sendResult is {} ", stringSendResult.toString());
        log.info("After the Send Library event");
        return new ResponseEntity<>(libraryEvent, HttpStatus.CREATED);
    }

    /**
     * Here, if the call is for post enpoint then we will create a new entry in database.
     * @param libraryEvent
     * @return
     * @throws JsonProcessingException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @RequestMapping(value = "/async2/libraryupdate",method = RequestMethod.POST)
    public ResponseEntity<LibraryEvent> postLibraryEventAsyncApproach2(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        log.info("Before the Send Library event");
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventAsynchronousApproach2(libraryEvent);
        log.info("After the Send Library event");
        return new ResponseEntity<>(libraryEvent, HttpStatus.CREATED);
    }


    /**
     * Here, if the call is for put endpoint then we will just update the entry in database.
     * @param libraryEvent
     * @return
     * @throws JsonProcessingException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @RequestMapping(value = "/async2/libraryupdate",method = RequestMethod.PUT)
    public ResponseEntity<LibraryEvent> updateLibraryEventAsyncApproach2(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        log.info("Before the Send Library event");
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducer.sendLibraryEventAsynchronousApproach2(libraryEvent);
        log.info("After the Send Library event");
        return new ResponseEntity<>(libraryEvent, HttpStatus.CREATED);
    }
}
