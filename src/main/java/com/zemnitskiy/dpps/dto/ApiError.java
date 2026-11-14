package com.zemnitskiy.dpps.dto;

/**
 * Body of every error response.
 *
 * @param status  HTTP status code
 * @param error   short error category, e.g. "Bad Request"
 * @param message what went wrong, written for the API client (never contains internal details)
 */
public record ApiError(int status, String error, String message) {
}
