package com.zemnitskiy.dpps.unit;

import com.zemnitskiy.dpps.security.KeycloakJwtAuthenticationConverter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("KeycloakJwtAuthenticationConverter")
@Tag("unit")
class KeycloakJwtAuthenticationConverterTest {

    private final KeycloakJwtAuthenticationConverter converter = new KeycloakJwtAuthenticationConverter();

    private static Jwt jwt(Consumer<Map<String, Object>> claims) {
        return Jwt.withTokenValue("token")
                .header("alg", "RS256")
                .subject("8a1b-uuid")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(60))
                .claims(claims)
                .build();
    }

    @Test
    @DisplayName("maps realm roles to ROLE_ authorities and uses the Keycloak username as principal name")
    void rolesAndUsername() {
        AbstractAuthenticationToken authentication = converter.convert(jwt(claims -> {
            claims.put("preferred_username", "alice");
            claims.put("realm_access", Map.of("roles", List.of("payments-read", "payments-write", "offline_access")));
        }));

        assertThat(authentication.getName()).isEqualTo("alice");
        assertThat(authentication.getAuthorities()).extracting(GrantedAuthority::getAuthority)
                .containsExactlyInAnyOrder("ROLE_payments-read", "ROLE_payments-write", "ROLE_offline_access");
    }

    @Test
    @DisplayName("grants nothing when the token carries no realm roles")
    void noRoles() {
        AbstractAuthenticationToken authentication = converter.convert(jwt(claims -> claims.put("scope", "profile")));

        assertThat(authentication.getAuthorities()).isEmpty();
        assertThat(authentication.getName()).isEqualTo("8a1b-uuid");
    }

    @Test
    @DisplayName("ignores malformed role claims")
    void malformedRoles() {
        AbstractAuthenticationToken authentication = converter.convert(jwt(claims ->
                claims.put("realm_access", Map.of("roles", List.of("payments-read", 42)))));

        assertThat(authentication.getAuthorities()).extracting(GrantedAuthority::getAuthority)
                .containsExactly("ROLE_payments-read");
        assertThat(converter.convert(jwt(claims -> claims.put("realm_access", Map.of("roles", "payments-read"))))
                .getAuthorities()).isEmpty();
    }
}
