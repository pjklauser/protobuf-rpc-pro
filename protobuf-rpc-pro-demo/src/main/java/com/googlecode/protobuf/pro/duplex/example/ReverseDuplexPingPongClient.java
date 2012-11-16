package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.Service;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;

public class ReverseDuplexPingPongClient {

	private static Log log = LogFactory.getLog(RpcClient.class);
	
    public static void main(String[] args) throws Exception {
		if ( args.length != 6 ) {
			System.err.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort> <processingTimeMs> <compress Y/N>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);

		int procTime = Integer.parseInt(args[4]);
		boolean compress = "Y".equals(args[5]);
		
		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);
    	
    	DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
        		client, 
        		new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));
        bootstrap.setCompression(compress);
		bootstrap.setRpcServerCallExecutor(new ThreadPoolCallExecutor(3, 10));			
        
        // Configure the client.

    	Service pongService = PongService.newReflectiveService(new PingPongServiceFactory.NonBlockingPongServer());
    	bootstrap.getRpcServiceRegistry().registerService(pongService);
    	
        // Set up the event pipeline factory.
    	bootstrap.setOption("connectTimeoutMillis",10000);
        bootstrap.setOption("connectResponseTimeoutMillis",10000);
        bootstrap.setOption("sendBufferSize", 1048576);
        bootstrap.setOption("receiveBufferSize", 1048576);
        bootstrap.setOption("tcpNoDelay", false);

        CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(bootstrap);
        
        // setup a RPC event listener - it just logs what happens
        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
        RpcConnectionEventListener listener = new RpcConnectionEventListener() {
			
			@Override
			public void connectionReestablished(RpcClientChannel clientChannel) {
				log.info("connectionReestablished " + clientChannel);
			}
			
			@Override
			public void connectionOpened(RpcClientChannel clientChannel) {
				log.info("connectionOpened " + clientChannel);
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
        
    	@SuppressWarnings("unused")
		RpcClientChannel channel = null;
		try {
	    	channel = bootstrap.peerWith(server);
			//we get called from the server - just wait for calls.
			Thread.sleep(procTime);
			
		} finally {
			System.exit(0);
		}
    }
}
