package com.learnkafka.kafkalearning.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.jnlp.IntegrationService;

/**
 * @author Sonu Kumar
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {
    private Integer bookId;
    private String bookName;
    private String bookAuthor;
}
