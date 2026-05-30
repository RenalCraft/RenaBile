# Шаг 1: Сборка проекта
FROM gradle:7.6-jdk17 AS build
WORKDIR /app

# Копируем только файлы конфигурации Gradle
COPY build.gradle settings.gradle* ./

# Копируем исходный код
COPY src ./src

# Собираем JAR-файл, используя глобальный gradle контейнера, игнорируя локальный gradlew
RUN gradle jar --no-daemon --stacktrace

# Шаг 2: Запуск приложения
FROM openjdk:17-jdk-slim
WORKDIR /app
EXPOSE 8080

# Копируем собранный jar-файл из предыдущего шага
COPY --from=build /app/build/libs/server.jar ./server.jar

ENTRYPOINT ["java", "-jar", "server.jar"]
