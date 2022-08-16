package com.techienotes.domain;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Rate {
    private Timestamp timestamp;
    private Double value;
    private String stock;
}
