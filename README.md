# protobuf-rpc-pro

This project provides an java implementation for Google's Protocol Buffer RPC services. The implementation builds upon Netty for low-level NIO.

## Features ##

  * TCP connection keep-alive.
  * Bi-directional RPC calls from client to server and server to client.
  * SSL socket layer encryption option.
  * Data Compression option.
  * RPC call cancellation.
  * RPC call timeout.
  * Out-of-Band RPC server replies to client.
  * Non RPC Protocol Buffer messaging for peer to peer communication.
  * Protocol Buffer wire protocol.
  * Semantics for calls handling on TCP connection closure.
  * Pluggable logging facility.


## WIKI / Documentation ##

for more information switch to the WIKI branch in GitHub and view the documentation.

## Maven Dependency ##

protobuf-rpc-pro is available via the maven central repository http://repo1.maven.org/maven2. The demo examples are available under the artifactId "protocol-rpc-pro-demo".

```
		<dependency>
			<groupId>com.googlecode.protobuf-rpc-pro</groupId>
			<artifactId>protobuf-rpc-pro-duplex</artifactId>
			<version>3.3.4</version>
			<type>jar</type>
		</dependency>
```

