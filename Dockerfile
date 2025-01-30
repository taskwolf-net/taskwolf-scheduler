FROM openjdk:21

COPY /build/libs/scheduler-1.0.0-SNAPSHOT.jar scheduler.jar
COPY /locale/ /locale/