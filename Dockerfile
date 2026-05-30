# Шаг 1: Собираем проект с помощью Gradle на базе Java 17
FROM gradle:7.6-jdk17 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle jar --no-daemon

# Шаг 2: Запускаем готовый jar на легковесной Java 17
FROM openjdk:17-jdk-slim
EXPOSE 8080
COPY --from=build /home/gradle/src/build/libs/server.jar /app/server.jar
ENTRYPOINT ["java", "-jar", "/app/server.jar"]
