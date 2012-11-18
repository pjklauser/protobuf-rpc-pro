package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Service;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;

public class DuplexPingPongServer {

	private static Log log = LogFactory.getLog(DuplexPingPongServer.class);

    public static void main(String[] args) throws Exception {
		if ( args.length != 8 ) {
			System.err.println("usage: <serverHostname> <serverPort> <pong=Y/N> <blocking=Y/N> <ssl=Y/N> <nodelay=Y/N> <pongDurationMs> <pongTimeoutMs>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		boolean pong = args[2].equalsIgnoreCase("Y");
		boolean blocking = args[3].equalsIgnoreCase("Y");
		boolean secure = args[4].equalsIgnoreCase("Y");
		boolean nodelay = args[5].equalsIgnoreCase("Y");
		int pongDurationMs = Integer.parseInt(args[6]);
		int pongTimeoutMs = Integer.parseInt(args[7]);
		
		log.info("DuplexPingPongServer " + serverHostname + ":"+ serverPort +" pong=" + (pong?"Y":"N")+ " blocking=" + (blocking?"Y":"N")+ " ssl=" + (secure?"Y":"N") + " nodelay=" + (nodelay?"Y":"N")+ " pongDurationMs="+pongDurationMs+" pongTimeoutMs="+pongTimeoutMs);
		
    	PeerInfo serverInfo = new PeerInfo(serverHostname, serverPort);

    	RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 10);
    	
        // Configure the server.
        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
        		serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool( new RenamingThreadFactoryProxy("boss", Executors.defaultThreadFactory()) ),
                        Executors.newCachedThreadPool(new RenamingThreadFactoryProxy("worker", Executors.defaultThreadFactory()) )));
        bootstrap.setRpcServerCallExecutor(executor);

        if ( secure ) {
        	RpcSSLContext sslCtx = new RpcSSLContext();
        	sslCtx.setKeystorePassword("changeme");
        	sslCtx.setKeystorePath("./lib/server.keystore");
        	sslCtx.setTruststorePassword("changeme");
        	sslCtx.setTruststorePath("./lib/truststore");
        	sslCtx.init();
        	
        	bootstrap.setSslContext(sslCtx);
        }
        bootstrap.setOption("sendBufferSize", 1048576);
        bootstrap.setOption("receiveBufferSize", 1048576);
        bootstrap.setOption("child.receiveBufferSize", 1048576);
        bootstrap.setOption("child.sendBufferSize", 1048576);
        bootstrap.setOption("tcpNoDelay", nodelay);
        
        RpcTimeoutExecutor timeoutExecutor = new TimeoutExecutor(1,5);
		RpcTimeoutChecker timeoutChecker = new TimeoutChecker();
		timeoutChecker.setTimeoutExecutor(timeoutExecutor);
		timeoutChecker.startChecking(bootstrap.getRpcClientRegistry());

		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(bootstrap);
        shutdownHandler.addResource(executor);
        shutdownHandler.addResource(timeoutChecker);
        shutdownHandler.addResource(timeoutExecutor);
        
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

    	if ( blocking ) {
    		if ( pong ) {
            	BlockingService pingService = PingService.newReflectiveBlockingService(new PingPongServiceFactory.BlockingPongingPingServer(pongDurationMs,pongTimeoutMs));
            	bootstrap.getRpcServiceRegistry().registerBlockingService(pingService);
    		} else {
            	BlockingService pingService = PingService.newReflectiveBlockingService(new PingPongServiceFactory.BlockingPingService());
            	bootstrap.getRpcServiceRegistry().registerBlockingService(pingService);
    		}
    	} else {
    		if ( pong ) {
            	Service pingService = PingService.newReflectiveService(new PingPongServiceFactory.NonBlockingPongingPingServer(pongDurationMs,pongTimeoutMs));
            	bootstrap.getRpcServiceRegistry().registerService(pingService);
    		} else {
            	Service pingService = PingService.newReflectiveService(new PingPongServiceFactory.NonBlockingPingServer());
            	bootstrap.getRpcServiceRegistry().registerService(pingService);
    		}
    	}
    	
    	// Bind and start to accept incoming connections.
        bootstrap.bind();
        
        log.info("Serving " + serverInfo);
        /*
        Thread.sleep(10000);   
        System.exit(0);
        */
        
    }
    
    
}
