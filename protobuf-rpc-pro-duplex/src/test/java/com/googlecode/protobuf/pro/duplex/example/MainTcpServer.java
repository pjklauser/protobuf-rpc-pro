package com.googlecode.protobuf.pro.duplex.example;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.SameThreadExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.googlecode.protobuf.pro.duplex.server.RpcClientConnectionRegistry;

public class MainTcpServer {
	public static void main(String[] args) throws IOException {
    	PeerInfo serverInfo = new PeerInfo("server'sHostname", 8080);
    	
//    	SameThreadExecutor executor = new SameThreadExecutor();
    	ThreadPoolCallExecutor executor = new ThreadPoolCallExecutor(10, 10);
    	
        // Configure the server.
        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
        		serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()),
                executor);

    	RpcClientConnectionRegistry eventLogger = new RpcClientConnectionRegistry();
    	bootstrap.registerConnectionEventListener(eventLogger);

    	bootstrap.getRpcServiceRegistry().registerService(new DefaultPingPongServiceImpl());
    	
    	// Bind and start to accept incoming connections.
        Channel c = bootstrap.bind();
	}
	
}
