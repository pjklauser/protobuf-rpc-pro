package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.googlecode.protobuf.pro.duplex.server.RpcClientConnectionRegistry;

public class MainTcpServer {
	
	private static Log log = LogFactory.getLog(MainTcpServer.class);
	
	public static void main(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.println("usage: <serverHostname> <serverPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		
    	PeerInfo serverInfo = new PeerInfo(serverHostname, serverPort);
    	
        // Configure the server.
        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
        		serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()),
                        new ThreadPoolCallExecutor(10, 10));

		// give the bootstrap to the shutdown handler so it is shutdown cleanly.
		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
		shutdownHandler.addResource(bootstrap);
		
    	RpcClientConnectionRegistry clientRegistry = new RpcClientConnectionRegistry();
    	bootstrap.registerConnectionEventListener(clientRegistry);

    	bootstrap.getRpcServiceRegistry().registerService(new DefaultPingPongServiceImpl());
    	
    	// Bind and start to accept incoming connections.
        bootstrap.bind();

        log.info("Handling " + serverInfo);
        
	}
	
}
