FROM openjdk:17-jdk-slim
WORKDIR /app
EXPOSE 8080
COPY build/libs/server.jar ./server.jar
ENTRYPOINT ["java", "-jar", "server.jar"]
