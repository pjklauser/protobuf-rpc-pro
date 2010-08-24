package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;

public class DuplexPingPongClient {

	private static Log log = LogFactory.getLog(RpcClient.class);
	
    public static void main(String[] args) throws Exception {
		if ( args.length != 7 ) {
			System.err.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort> <numCalls> <processingTimeMs> <payloadBytes>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);

		int numCalls = Integer.parseInt(args[4]);
		int procTime = Integer.parseInt(args[5]);
		int payloadSize = Integer.parseInt(args[6]);
		
		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);
    	
		RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 10);
		
    	DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
        		client, 
        		new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()),
                executor);
        
        // Configure the client.

    	Service pongService = PongService.newReflectiveService(new DefaultPingPongServiceImpl());
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
        
    	RpcClientChannel channel = null;
		try {
	    	channel = bootstrap.peerWith(server);
			BlockingInterface myService = PingService.newBlockingStub(channel);
			
	    	long startTS = 0;
	    	long endTS = 0;
	    	
			startTS = System.currentTimeMillis();

			for( int i = 0; i < numCalls; i++ ) {
				if ( i % 100 == 1 ) {
					System.out.println(i);
				}
				RpcController controller = channel.newRpcController();
				
				ByteString requestData = ByteString.copyFrom(new byte[payloadSize]);
				Ping ping = Ping.newBuilder().setNumber(procTime).setPingData(requestData).build();
				Pong pong = myService.ping(controller, ping);
				if ( pong.getPongData().size() != payloadSize ) {
					throw new Exception("Reply payload mismatch.");
				}
				if ( pong.getNumber() != ping.getNumber() ) {
					throw new Exception("Reply number mismatch.");
				}
			}
			endTS = System.currentTimeMillis();
			log.info("BlockingCalls " + numCalls + " in " + (endTS-startTS)/1000 + "s");
			
		} finally {
			System.exit(0);
		}
    }
}
