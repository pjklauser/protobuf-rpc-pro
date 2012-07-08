package com.googlecode.protobuf.pro.duplex.example.nonrpc;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.example.PingPong;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Status;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;

public class StatusServer {
	
	private static Log log = LogFactory.getLog(StatusServer.class);
	
	public static void main(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.println("usage: <serverHostname> <serverPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		
    	PeerInfo serverInfo = new PeerInfo(serverHostname, serverPort);
    	
		// RPC payloads are uncompressed when logged - so reduce logging
		CategoryPerServiceLogger logger = new CategoryPerServiceLogger();
		logger.setLogRequestProto(false);
		logger.setLogResponseProto(false);

		// Configure the server.
        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
        		serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()),
                new ThreadPoolCallExecutor(10, 10), 
                logger);

		
		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(bootstrap);
        
        final RpcCallback<PingPong.Status> clientStatusCallback = new RpcCallback<PingPong.Status>() {

			@Override
			public void run(PingPong.Status parameter) {
				log.info("Received " + parameter);
			}
        	
		};
        // setup a RPC event listener - it just logs what happens
        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
        RpcConnectionEventListener listener = new RpcConnectionEventListener() {
			
			@Override
			public void connectionReestablished(RpcClientChannel clientChannel) {
				log.info("connectionReestablished " + clientChannel);

				clientChannel.setOobMessageCallback(Status.getDefaultInstance(), clientStatusCallback);
			}
			
			@Override
			public void connectionOpened(RpcClientChannel clientChannel) {
				log.info("connectionOpened " + clientChannel);
				
				clientChannel.setOobMessageCallback(Status.getDefaultInstance(), clientStatusCallback);
			}
			
			@Override
			public void connectionLost(RpcClientChannel clientChannel) {
				log.info("connectionLost " + clientChannel);
			}
			
			@Override
			public void connectionChanged(RpcClientChannel clientChannel) {
				log.info("connectionChanged " + clientChannel);
			}
		};
		rpcEventNotifier.setEventListener(listener);
    	bootstrap.registerConnectionEventListener(rpcEventNotifier);

    	// Bind and start to accept incoming connections.
        bootstrap.bind();
        log.info("Serving " + bootstrap);
        
        while ( true ) {
            List<RpcClientChannel> clients = bootstrap.getRpcClientRegistry().getAllClients();
            for ( RpcClientChannel client : clients ) {
            	
            	PingPong.Status serverStatus = PingPong.Status.newBuilder()
            			.setMessage("Server "+ bootstrap.getServerInfo() + " OK@" + System.currentTimeMillis()).build();
            	
            	client.sendOobMessage(serverStatus);
            }
            log.info("Sleeping 5s before sending serverStatus to all clients.");
        	Thread.sleep(5000);
        }
	}
	
	
}
