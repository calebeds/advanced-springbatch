# Prerequisites

In order to run this Spring Batch application, the following pre-requisites need to be met

## Java 17

Java of version 17.
To check: `java --version`

Which should give result similar to
```shell
java 17.0.9 2023-10-17 LTS
Java(TM) SE Runtime Environment (build 17.0.9+11-LTS-201)
Java HotSpot(TM) 64-Bit Server VM (build 17.0.9+11-LTS-201, mixed mode, sharing)
```

## Maven 3.9.0

Ideal version is **3.9.9**, though versions close enough to 3.9.9 should work as well.
To check: `mvn --version`

The desired result is similar to
```shell
Apache Maven 3.9.9 
Maven home: /home/youruser/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9
Java version: 17.0.2, vendor: Oracle Corporation, runtime: /home/calebesantos/dev/jdk-17.0.2
Default locale: pt_BR, platform encoding: UTF-8
OS name: "linux", version: "6.5.0-35-generic", arch: "amd64", family: "unix"
```

## MySQL 8.0.x

Ideal version is **8.0.31** (the one it has been tested with), however versions close enough to 8.0.31 should work as well.
To check, connect to your instance: `mysql -u yourusername -h yourhostifnotlocal -p`
And in MySQL terminal, type: `select version();`

The desired result is similar to
```shell
+-----------+
| version() |
+-----------+
| 8.0.31    |
+-----------+
```

**!!!** One important step is to create empty database, which is easy to do from MySQL command-line:
```shell
create database mydatabasename;
```

# How-to

## Configurations

In order to run the team performance job, there are 3 properties that needs to be adjusted in **src/resources/application.properties** file: `db.url`, `db.username` and `db.password`. Assign the values that you configured. For example:

```properties
db.url=jdbc:mysql://localhost:3306/mydatabasename
db.username=root
db.password=kirylbatchpassword
```

## Build & run

The following lifecycle happens when operating this application:
1. Application needs to be built, meaning source files, configurations, etc.
2. Application needs to be started as a server application
3. Server application can be used through the endpoint to start Spring Batch jobs

### Build

In order to build the application, please run the following command from the root of the project: `mvn clean package`

### Server start

In order to start application server, please run the following command from the root of the project: `mvn spring-boot:run`

### Starting the job

To start the team performance job with specific score rank in mind, please run the following command to send HTTP POST request to the application server: `curl -X POST http://localhost:8080/start?scoreRank=0`

### Tests

To only run tests of the application, please run the following command from the root of the project: `mvn clean test`