package com.zemnitskiy.dpps.dto;

public record ErrorResponse(int status, String error, String message) {
}
