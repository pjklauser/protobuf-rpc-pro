# Netty #

Since protobuf-rpc-pro uses Netty to provide the io over TCP, optimization of Netty will help increase performance. Here is a good article giving ideas how to squeeze the most out of Netty. http://gleamynode.net/articles/2232/

JVM options
**-server -Xms2048m -Xmx2048m -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods**

Using the JVM options described increased throughput by 15% under certain conditions`*`.

# protobuf-rpc-pro configuration #

If you do not call a client RPC method from within the processing of a server side RPC call then configure your server side DuplexTcpServerBootstrap with a SameThreadExecutor. This can increase the throughput by 25%`*`, since the thread context switching is avoided.

`*` - for calls which require basically no processing time on the server side.

# Disabling Logging #

One way to reduce logging is to reduce the logging data of the logger registered at the DuplexTcpClientPipelineFactory or DuplexTcpServerPipelineFactory
.
```
 // RPC payloads are uncompressed when logged - so reduce logging
 CategoryPerServiceLogger logger = new CategoryPerServiceLogger();
 logger.setLogRequestProto(false);
 logger.setLogResponseProto(false);
 factory.setRpcLogger(logger);
```

or alternatively a com.googlecode.protobuf.pro.duplex.logging.NullLogger can be used instead of the CategoryPerServiceLogger which will not log anything.