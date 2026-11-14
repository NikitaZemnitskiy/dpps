package com.zemnitskiy.dpps.support;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Stand-in for Keycloak in tests: serves a JWK set over HTTP and signs access tokens shaped like Keycloak's
 * ({@code preferred_username}, {@code realm_access.roles}). The application validates them with its real
 * resource-server configuration - signature, expiry, issuer and audience.
 */
public final class TestJwtIssuer {

    public static final String ISSUER = "http://keycloak.test/realms/payment-system";
    public static final String AUDIENCE = "payment-service";

    private static final TestJwtIssuer INSTANCE = new TestJwtIssuer();

    private final RSAKey signingKey;
    private final HttpServer server;

    private TestJwtIssuer() {
        signingKey = newKey("test-signing-key");
        try {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        byte[] jwks = new JWKSet(signingKey.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
        server.createContext("/certs", exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, jwks.length);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(jwks);
            }
        });
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));
    }

    public static TestJwtIssuer get() {
        return INSTANCE;
    }

    public String jwkSetUri() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/certs";
    }

    /** A token builder with valid defaults: right issuer, audience and key, valid for 5 minutes, no roles. */
    public Token token(String username) {
        return new Token(username);
    }

    /** A key the application doesn't know (for tokens with a forged signature). */
    public static RSAKey newKey(String keyId) {
        try {
            return new RSAKeyGenerator(2048).keyID(keyId).generate();
        } catch (JOSEException e) {
            throw new IllegalStateException(e);
        }
    }

    public final class Token {

        private final String username;
        private List<String> roles = List.of();
        private String issuer = ISSUER;
        private String audience = AUDIENCE;
        private Instant expiresAt = Instant.now().plus(Duration.ofMinutes(5));
        private RSAKey key = signingKey;

        private Token(String username) {
            this.username = username;
        }

        public Token roles(String... roles) {
            this.roles = List.of(roles);
            return this;
        }

        public Token issuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        public Token audience(String audience) {
            this.audience = audience;
            return this;
        }

        public Token expiresAt(Instant expiresAt) {
            this.expiresAt = expiresAt;
            return this;
        }

        public Token signedWith(RSAKey key) {
            this.key = key;
            return this;
        }

        /** Value for the {@code Authorization} header. */
        public String bearer() {
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .issuer(issuer)
                    .audience(audience)
                    .subject("id-of-" + username)
                    .claim("preferred_username", username)
                    .claim("realm_access", Map.of("roles", roles))
                    .issueTime(Date.from(expiresAt.minus(Duration.ofMinutes(5))))
                    .expirationTime(Date.from(expiresAt))
                    .build();
            SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims);
            try {
                jwt.sign(new RSASSASigner(key));
            } catch (JOSEException e) {
                throw new IllegalStateException(e);
            }
            return "Bearer " + jwt.serialize();
        }
    }
}
