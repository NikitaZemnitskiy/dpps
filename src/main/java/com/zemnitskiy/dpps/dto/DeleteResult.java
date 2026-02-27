package com.zemnitskiy.dpps.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DeleteResult {
    private int deletedCount;
}
