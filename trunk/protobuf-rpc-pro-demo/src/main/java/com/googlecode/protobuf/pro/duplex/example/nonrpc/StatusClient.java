package com.googlecode.protobuf.pro.duplex.example.nonrpc;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.client.RpcClientConnectionWatchdog;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;

public class StatusClient {

	private static Log log = LogFactory.getLog(StatusClient.class);

	private static RpcClientChannel channel = null;
	
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err
					.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);

		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);

		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
		try {
			DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
					client, new NioClientSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool()));
			bootstrap.setRpcServerCallExecutor(new ThreadPoolCallExecutor(3, 10));			
			
			RpcClientConnectionWatchdog watchdog = new RpcClientConnectionWatchdog(bootstrap);
	        watchdog.start();

			// RPC payloads are uncompressed when logged - so reduce logging
			CategoryPerServiceLogger logger = new CategoryPerServiceLogger();
			logger.setLogRequestProto(false);
			logger.setLogResponseProto(false);
			bootstrap.setRpcLogger(logger);
			
			shutdownHandler.addResource(bootstrap);

			final RpcCallback<PingPong.Status> serverStatusCallback = new RpcCallback<PingPong.Status>() {

				@Override
				public void run(PingPong.Status parameter) {
					log.info("Received " + parameter);
				}
	        	
			};
			
			// Set up the event pipeline factory.
	        // setup a RPC event listener - it just logs what happens
	        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
	        
	        final RpcConnectionEventListener listener = new RpcConnectionEventListener() {
				
				@Override
				public void connectionReestablished(RpcClientChannel clientChannel) {
					log.info("connectionReestablished " + clientChannel);
					channel = clientChannel;
					channel.setOobMessageCallback(PingPong.Status.getDefaultInstance(), serverStatusCallback);
				}
				
				@Override
				public void connectionOpened(RpcClientChannel clientChannel) {
					log.info("connectionOpened " + clientChannel);
					channel = clientChannel;
					clientChannel.setOobMessageCallback(PingPong.Status.getDefaultInstance(), serverStatusCallback);
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
			rpcEventNotifier.addEventListener(watchdog);
			rpcEventNotifier.addEventListener(listener);
	    	bootstrap.registerConnectionEventListener(rpcEventNotifier);

			bootstrap.peerWith(server);
			
			while (true && channel != null) {
				
            	PingPong.Status clientStatus = PingPong.Status.newBuilder()
            			.setMessage("Client " + channel + " OK@" + System.currentTimeMillis()).build();
            	
            	channel.sendOobMessage(clientStatus);
				
				Thread.sleep(1000);
				
			}

		} finally {
			System.exit(0);
		}
	}

}
