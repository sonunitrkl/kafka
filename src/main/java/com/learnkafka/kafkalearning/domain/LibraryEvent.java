package com.learnkafka.kafkalearning.domain;

import com.learnkafka.kafkalearning.enums.LibraryEventType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Sonu Kumar
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {
    private LibraryEventType libraryEventType;
    private Integer libraryEventId;
    private Book book;
}
