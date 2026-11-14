package com.zemnitskiy.dpps.security;

/** Keycloak realm roles that grant access to the API. */
public final class Roles {

    /** Read payments and statistics. */
    public static final String READ = "payments-read";

    /** Upload and delete payments. */
    public static final String WRITE = "payments-write";

    private Roles() {
    }
}
