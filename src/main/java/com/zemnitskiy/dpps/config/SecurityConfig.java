package com.zemnitskiy.dpps.config;

import com.zemnitskiy.dpps.security.KeycloakJwtAuthenticationConverter;
import com.zemnitskiy.dpps.security.Roles;
import jakarta.servlet.DispatcherType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Stateless OAuth2 resource server: every API call needs a Keycloak access token whose signature, expiry,
 * issuer and audience are valid (see {@code spring.security.oauth2.resourceserver.jwt.*}). Each endpoint
 * additionally requires a realm role; anything not listed here is denied.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests(auth -> auth
                        // Re-dispatches of a request that was already authorized: the completion of a streamed
                        // response (ASYNC) and error rendering (ERROR). Denying them would only break responses
                        // that are already being sent.
                        .dispatcherTypeMatchers(DispatcherType.ASYNC, DispatcherType.ERROR).permitAll()
                        .requestMatchers(HttpMethod.GET, "/actuator/health", "/actuator/health/**").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/payments", "/api/statistics").hasRole(Roles.READ)
                        .requestMatchers(HttpMethod.POST, "/api/payments/upload").hasRole(Roles.WRITE)
                        .requestMatchers(HttpMethod.DELETE, "/api/payments").hasRole(Roles.WRITE)
                        .anyRequest().denyAll())
                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(new KeycloakJwtAuthenticationConverter())))
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                // Bearer tokens are never sent automatically by browsers, so CSRF protection has nothing to protect.
                .csrf(AbstractHttpConfigurer::disable);
        return http.build();
    }
}
