What changes are made from release to release. Check SVN history for more details.

# 3.3.4 #

[#49](https://github.com/pjklauser/protobuf-rpc-pro/issues/49) StackOverflowError DuplexTcpClientPipelineFactory.peerWith

[#48](https://github.com/pjklauser/protobuf-rpc-pro/issues/48) WatchdogThread is not renamed to a readable thread name

[#46](https://github.com/pjklauser/protobuf-rpc-pro/issues/46) CleanShutdownHandler leaves shutdown hook registered when shutdown explicitly

Upgrade to Netty 4.0.33.Final

# 3.3.3 #

[#45](https://github.com/pjklauser/protobuf-rpc-pro/issues/45) RpcClientChannel support "attributes" on initial connection (peerWith)

[#44](https://github.com/pjklauser/protobuf-rpc-pro/issues/44) CleanShutdownHandler explicit shutdown leaves non daemon threads hanging which prohibit later JVM shutdown.


# 3.3.2 #

[#37](https://github.com/pjklauser/protobuf-rpc-pro/pull/37) CleanShutdownHandler to support shutdown on demand.

[#42](https://github.com/pjklauser/protobuf-rpc-pro/issues/42) CleanShutdownHandler to shutdown RpcClientConnectionWatchdog.

[#43](https://github.com/pjklauser/protobuf-rpc-pro/issues/43) RpcClientChannel support "attributes" and isClosed method.

Upgrade to Netty 4.0.31.Final

# 3.3.1 #

Completed move from google code to GitHub.

Upgrade to Netty 4.0.27.Final

# 3.3 #

[Issue 34](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=34): upgrade to protobuf-java 2.6. Since the protoc 2.6 introduces new java stubs and requires an upgrade to protobuf-java 2.6, i've decided to call this a minor release upgrade, rather than a micro-release upgrade.

[Issue 35](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=35): provide a means to avoid logging. See NullLogger.

Upgrade to Netty 4.0.23.Final

# 3.2.3 #

Upgrade to Netty 4.0.19.Final

[Issue 33](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=33): added remove blocking service from RpcServiceRegistry.

# 3.2.2 #

Upgrade to Netty 4.0.15.Final

[Issue 31](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=31): debug loggig for timeout checker

[Issue 25](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=25): refix to wait correct number of nanos on client timeout.

# 3.2.1 #

[Issue 29](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=29): RpcServiceRegistry to use service's fullName for indexing to avoid duplicate name registration problems.
(don't use 3.2 it is broken for this issue)

[Issue 30](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=30): changed the constructor of DuplexTcpClientPipelineFactory to not require a clientInfo for the local address. If a clientInfo is provided, by calling #setClientInfo instead, then a strict local port binding is performed ( and the factory can only create one connection at a time with #peerWith ). Probably you don't need this and can never set the clientInfo explicitly, then you get a new free local port used for each connection created with #peerWith.

# 3.1.0 #

[Issue 28](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=28): Fix issues with protobuf extension registry to decode extensions.

Upgrade to Netty.4.0.14.Final

# 3.0.9 #

[Issue 23](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=23): Depend on specific netty modules instead of netty-all to get better OSGI bundling.

Upgrade to Netty.4.0.13.Final

# 3.0.8 #

[Issue 22](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=22): Allow use of JdkZLib compression as an alternative to JZLib. The JZLib dependency is optional now.

Upgrade to Netty.4.0.10.Final

# 3.0.7 #

[Issue 25](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=25): Fix part3 - integer overflow in timeout calculation ( don't use 3.0.6 ).

# 3.0.6 #

[Issue 25](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=25): Fix part2 - timeout after microseconds instead of nanoseconds.

# 3.0.5 #
Upgrade to Java1.7 compile and bytecode

[Issue 23](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=23): Upgrade to Netty 4.0.6.Final

[Issue 25](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=25): Fix infinite loop after timeout on RpcClient.callBlockingMethod.

# 3.0.4 #
[Issue 11](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=11): Provided OSGI bundle.
Upgraded to Netty 4.0.0.CR1.

# 3.0.3 #
[Issue 16](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=16): Upgrade to Netty 4.0.0.Beta2. There is a difference in the Bootstrap code which clients and servers use to construct their components. Checkout the changed examples.

[Issue 19](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=19): Upgrade dependency to Protobuf 2.5. New protoc idl compiler required for clients.

Out-of-Band messaging now allows client code do success evaluation and/or blocking until completion. See RpcClientChannel#sendOobMessage and ServerRpcController#sendOobResponse methods returning ChannelFuture .

# 3.0.2 #

[Issue 17](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=17): stackoverflow - infinite loop calling RpcClient.getPipeline().

# 3.0.1 #

[Issue 10](https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=10): replace commons logging with slf4j-api. Demo classes use logback-classic dependency configured by VM arguments adding system property -Dlogback.configurationFile="./lib/logback.xml".

Dependency Update: upgraded to Netty version 3.6.0 Final.

# 3.0.0 #

Feature: RPC timeout. See wiki page "RpcTimeout" for more info.

Fix: refactor Bootstrap client and server classes to be more symetric.

Feature: new utility AvailablePortFinder utility to allow selection of availible server TCP ports for PeerInfo.

Fix 14: replacement of PeerInfo's PID with UUID.

Dependency Update: upgraded to Netty version 3.5.10 Final.

# 2.0.1 #

Dependency Update: upgraded to Netty version 3.5.3 Final.

Fix: don't allow further sending over closed RpcClient, so closure is not starved.

# 2.0.0 #

Feature: Out-of-Band Protobuf Messaging between peers.

Feature: Out-of-Band RPC server replies

Feature: provide "transparent" messages through Netty pipeline

Feature: allow registration of BlockingServices in servers.

Feature: provide easy access to the Netty pipeline.

Feature: Provide WirePayload ( wire protocol ) extension possibility.

Fix#9: reversed logging roles

Discontinued: protobuf-streamer-pro is discontinued since the Out-of-Band messaging features can be used to allow pull of large files from Server to Client, making the separate library obsolete.

Dependency Update: Netty 3.5.2.Final

# 1.2.2 #

Fix#8: fix for hanging due to close race condition.

# 1.2.1 #

Fix: improve asynchronicity of reply calls.

# 1.2.0 #

Feature: Introduced compression

Made availible on maven central repository.

# 1.0.0 #

Initial version.
