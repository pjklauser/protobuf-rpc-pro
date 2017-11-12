---
date : 2016-09-18T14:57:52+02:00
draft : false
title : Runtime Dependencies
---
The external dependencies have been kept to a minimum. Netty, slf4j and protobuf-java are the only compile time dependencies. Compiled against java 1.7. The dependencies can be seen in the project's maven pom.xml.

```
		<dependency>
			<groupId>com.google.protobuf</groupId>
			<artifactId>protobuf-java</artifactId>
			<version>2.6.1</version>
		</dependency>
		<dependency>
			<groupId>io.netty</groupId>
			<artifactId>netty</artifactId>
			<version>4.0.23.Final</version>
		</dependency>
		<dependency>
			<groupId>org.slf4j</groupId>
			<artifactId>slf4j-api</artifactId>
			<version>1.7.2</version>
		</dependency>
```