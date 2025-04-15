Deployment:
Configuration: Upload WAR file to EC2 instance, and modify the URL path in the MultiThreadedLiftRideClient class: private static final String SERVER_URL.

Lauch new EC2 instance running Ubuntu and install Redis. Change Line 23 private static final String DBHost to EC2 instance IP address.

Change variable HOST in SkierConsumer and ServerAPI accordinging to EC2 instance ip address that host Rabbitmq.

To run the project, start the ServerAPI project, the Client project and Consumer project separately, as they are built independently using Maven. To run Consumer, upload jar file to EC2 instance and run java -jar Consumer-1.0-SNAPSHOT.jar.
