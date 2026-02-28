package com.zemnitskiy.dpps.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Payment implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String id;
    private String dateTime;
    private String sender;
    private String receiver;
    private double value;
}
