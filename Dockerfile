FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
EXPOSE 8080
COPY build/libs/server.jar ./server.jar
ENTRYPOINT ["java", "-jar", "server.jar"]
