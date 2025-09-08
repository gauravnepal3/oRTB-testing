# Stage 1: Build the app using Maven
FROM maven:3.9.11-amazoncorretto-21 AS build
WORKDIR /app

# Copy Maven files first to leverage caching
COPY pom.xml .
COPY mvnw .
COPY .mvn .mvn

# Download dependencies
RUN mvn dependency:go-offline

# Copy source code
COPY src ./src

# Build the Spring Boot app (skip tests for faster builds)
RUN mvn clean package -DskipTests

# Stage 2: Run the app using lightweight JRE
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Copy the JAR from build stage
COPY --from=build /app/target/*.jar app.jar

# Optional: Set memory limits for Java
ENV JAVA_OPTS="-Xms512m -Xmx1024m"

# Run the app
CMD ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
