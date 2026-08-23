# ---- 构建阶段 ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY . .
RUN mvn -q -B -DskipTests package

# ---- 运行阶段 ----
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/herald-examples/server-example/target/server-example-*.jar /app/server.jar
COPY --from=build /build/herald-examples/producer-example/target/producer-example-*.jar /app/producer.jar
COPY --from=build /build/herald-examples/consumer-example/target/consumer-example-*.jar /app/consumer.jar
COPY docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENV HERALD_SERVICE=server
ENTRYPOINT ["docker-entrypoint.sh"]
