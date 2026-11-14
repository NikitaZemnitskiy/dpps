# --- Build stage: dependencies are resolved in their own layer, so source changes rebuild quickly ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml lombok.config ./
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

# --- Runtime stage: JRE only, unprivileged user ---
FROM eclipse-temurin:17-jre
RUN groupadd --system dpps && useradd --system --gid dpps --no-create-home --home-dir /app dpps
WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
# Ignite's JVM flags - the same file `mvn test` uses.
COPY jvm.options jvm.options
USER dpps

# 8080 HTTP API, 47100 Ignite communication, 47500 Ignite discovery
EXPOSE 8080 47100 47500

# Bash's /dev/tcp instead of curl: nothing extra has to be installed in the image.
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=12 \
    CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8080 && printf 'GET /actuator/health HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' >&3 && grep -q '\"status\":\"UP\"' <&3"]

# Heap size is set by the deployment through JDK_JAVA_OPTIONS (see docker-compose.yml).
ENTRYPOINT ["java", "@/app/jvm.options", "-jar", "/app/app.jar"]
