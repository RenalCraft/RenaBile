FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
EXPOSE 8080
# Теперь Gradle собирает файл сразу с именем server.jar в папку build/libs/
COPY build/libs/server.jar ./server.jar
ENTRYPOINT ["java", "-jar", "server.jar"]
