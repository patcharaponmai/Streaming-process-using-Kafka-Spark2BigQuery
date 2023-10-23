# Use the official Bitnami Spark image as the base image
FROM bitnami/spark:latest

# Set the JAVA_HOME environment variable to the Java installation path
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64/

# Install the default OpenJDK 11 package for AMD64 architecture
USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Create the missing directory with superuser privileges
RUN mkdir -p /usr/lib/jvm/java-11-openjdk-arm64/

# Clean up the package cache
RUN apt-get clean

# Make sure Spark uses the installed Java
RUN echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64/' >> /opt/bitnami/spark/conf/spark-env.sh

# Switch back to a non-root user (optional, but recommended for security)
USER $NB_UID

# Start the Spark entrypoint (command)
CMD ["/opt/bitnami/scripts/spark/entrypoint.sh"]
