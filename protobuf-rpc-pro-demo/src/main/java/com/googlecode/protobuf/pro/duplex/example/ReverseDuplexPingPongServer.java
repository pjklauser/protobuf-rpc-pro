package com.googlecode.protobuf.pro.duplex.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;

public class ReverseDuplexPingPongServer {

	private static Log log = LogFactory.getLog(ReverseDuplexPingPongServer.class);

    public static void main(String[] args) throws Exception {
		if ( args.length != 3 ) {
			System.err.println("usage: <serverHostname> <serverPort> <clientProcTime>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		int clientProcTime = Integer.parseInt(args[2]);
		
    	PeerInfo serverInfo = new PeerInfo(serverHostname, serverPort);
    	
    	RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 10);
    	
        // Configure the server.
        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
        		serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()),
                executor);

        bootstrap.setOption("sendBufferSize", 1048576);
        bootstrap.setOption("receiveBufferSize", 1048576);
        bootstrap.setOption("child.receiveBufferSize", 1048576);
        bootstrap.setOption("child.sendBufferSize", 1048576);
        bootstrap.setOption("tcpNoDelay", false);
        
		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(bootstrap);
        
        final List<RpcClientChannel> clients = new ArrayList<RpcClientChannel>();
        
        // setup a RPC event listener - it just logs what happens
        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
        RpcConnectionEventListener listener = new RpcConnectionEventListener() {
			
			@Override
			public void connectionReestablished(RpcClientChannel clientChannel) {
				log.info("connectionReestablished " + clientChannel);
				clients.add(clientChannel);
			}
			
			@Override
			public void connectionOpened(RpcClientChannel clientChannel) {
				log.info("connectionOpened " + clientChannel);
				clients.add(clientChannel);
			}
			
			@Override
			public void connectionLost(RpcClientChannel clientChannel) {
				log.info("connectionLost " + clientChannel);
				clients.remove(clientChannel);
			}
			
			@Override
			public void connectionChanged(RpcClientChannel clientChannel) {
				log.info("connectionChanged " + clientChannel);
				clients.add(clientChannel);
			}
		};
		rpcEventNotifier.setEventListener(listener);
    	bootstrap.registerConnectionEventListener(rpcEventNotifier);

    	// Bind and start to accept incoming connections.
        bootstrap.bind();
        
        log.info("Serving " + serverInfo);
        
        try {
            while( true ) {
            	if ( !clients.isEmpty() ) {
                	List<RpcClientChannel> clone = new ArrayList<RpcClientChannel>();
                	clone.addAll(clients);
            		
            		for( RpcClientChannel channel : clone ) {
                		BlockingInterface clientService = PongService.newBlockingStub(channel);
                		RpcController clientController = channel.newRpcController();

                		try {
                			Pong clientRequest = Pong.newBuilder().setNumber(clientProcTime).setPongData(ByteString.copyFromUtf8("hello world")).build();

                			Ping clientResponse = clientService.pong(clientController, clientRequest);
                		} catch ( ServiceException e ) {
                			log.warn("Failed." , e);
                		}
            			
            		}
            	} else {
            		Thread.sleep(1000);
            		log.info("No clients.");
            	}
            	
            }
        } catch ( Throwable t ) {
        	log.error("Throwable ", t);
        } finally {
        	System.exit(-1);
        }
    }
    
}
