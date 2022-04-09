package com.learnkafka.kafkalearning.domain;

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
    private Integer libraryEventId;
    private Book book;
}
