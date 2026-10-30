# Stage 1: Build the application
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /app
COPY .mvn/ .mvn
COPY mvnw pom.xml ./
RUN chmod +x ./mvnw
# Pre-fetch dependencies
RUN ./mvnw dependency:go-offline -B
COPY src ./src
RUN ./mvnw clean package -DskipTests

# Stage 2: Run the application
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar
EXPOSE 8081
ENV JAVA_OPTS="-Xms256m -Xmx768m -XX:+UseG1GC" NODE_ID=local
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]