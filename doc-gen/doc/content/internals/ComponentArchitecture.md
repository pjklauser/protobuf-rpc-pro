---
date : 2016-09-18T14:57:52+02:00
draft : false
title : Architecture
---
The protobuf-rpc-pro libraries primary function is to enable fully duplex RPC calls multiplexed over a single TCP/IP socket connection. Due to this duplex nature, the component architecture is almost symetric on client and server sides. The picture below shows the libraries internal architecture.

![Architecture Diagram](https://raw.githubusercontent.com/pjklauser/protobuf-rpc-pro/master/protobuf-rpc-pro-duplex/doc/protobuf-rpc-pro.png)


  * DuplexTcpClient/ServerBootstraps - These are the factories for client and server connections.
    * PeerInfo -  The identity of the client and server respectively, provided by hostname and port which is bound.
    * ChannelFactory - Netty's configurer of the NIO socket layer and the Thread factories which handle the low level IO activities.
    * RpcLogger - The logging implementation which logs each call.
    * ChannelPipelineFactory - An implementation of Netty's ChannelPipelineFactory which sets up the Netty channel for clients connecting to servers, and for the server when clients connect.
  * RpcClient - the primary object which a client will use, which implements the google protobuf API's RpcClientChannel. The RpcClient keeps track of which RPC calls are ongoing towards the RpcServer at the other end of the Channel. Each RPC messages sent from client to server has a correlationId which is unique and increasing for the Channel.
  * RpcServer - the object calls a previously registered RpcService at the RpcServiceRegistry using the RpcServerCallExecutor, when RPC client calls come up the Channel. The RpcServer keeps a map of the ongoing RPC calls which are being processed. This way, it is possible to interrupt and cancel ongoing server calls on receipt of client cancellations.
  * Channel - the Netty channel representing a bidirectional Socket connection between client and server peers. The Channel is optionally configured with encryption and compression. The Channel handles serialization and deserialization of the WirePayload which is a protobuf format for RPC or Oob messages between the client and server.
