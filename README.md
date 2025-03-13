Deployment:
Configuration: Upload WAR file to EC2 instance, and modify the URL path in the MultiThreadedLiftRideClient class: private static final String SERVER_URL


Configuration of Load Balancer: An Elastic Load Balancer (ELB) is deployed in front of two EC2 instances to distribute incoming traffic efficiently. This enhances the servlet application's availability and fault tolerance. Modify the URL path in the MultiThreadedLiftRideClient class: private static final String SERVER_URL to point to the Load Balancer DNS


To run the project, start the ServerAPI project, the Client project and Consumer project separately, as they are built independently using Maven. To run Consumer, upload jar file to EC2 instance and run java -jar Consumer-1.0-SNAPSHOT.jar. Change HOST in 
SkierConsumer and ServerAPI accordinging to EC2 instance ip address that host Rabbitmq.
