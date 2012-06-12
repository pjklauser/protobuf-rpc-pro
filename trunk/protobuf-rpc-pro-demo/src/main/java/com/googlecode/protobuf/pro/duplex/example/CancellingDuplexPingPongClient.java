package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
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
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;

public class CancellingDuplexPingPongClient {

	private static Log log = LogFactory.getLog(RpcClient.class);
	
    public static void main(String[] args) throws Exception {
		if ( args.length != 8 ) {
			System.err.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort> <numCalls> <processingTimeMs> <payloadBytes> <compress Y/N>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);

		int numCalls = Integer.parseInt(args[4]);
		int procTime = Integer.parseInt(args[5]);
		final int payloadSize = Integer.parseInt(args[6]);
		boolean compress = "Y".equals(args[7]);
		
		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);
    	
		RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 10);
		
    	DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
        		client, 
        		new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()),
                executor);
        bootstrap.setCompression(compress);
        
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
	    	PingService myService = PingService.newStub(channel);
			
	    	long startTS = 0;
	    	long endTS = 0;
	    	
			startTS = System.currentTimeMillis();

			for( int i = 0; i < numCalls; i++ ) {
				if ( i % 100 == 1 ) {
					System.out.println(i);
				}
				final RpcController controller = channel.newRpcController();
				
				ByteString requestData = ByteString.copyFrom(new byte[payloadSize]);
				final Ping ping = Ping.newBuilder().setNumber(procTime).setPingData(requestData).build();
				myService.ping(controller, ping, new RpcCallback<PingPong.Pong>() {
					
					@Override
					public void run(Pong pong) {
						log.info("We got a " + pong);
						if ( controller.failed() ) {
							log.info("The call failed. " + controller.errorText());
						}
						if ( pong != null ) {
							if ( pong.getPongData().size() != payloadSize ) {
								log.error("Reply payload mismatch.");
							}
							if ( pong.getNumber() != ping.getNumber() ) {
								log.error("Reply number mismatch.");
							}
						}						
					}
				});
				Thread.sleep(procTime / 2);
				controller.startCancel();
				Thread.sleep(procTime * 2);
			}
			endTS = System.currentTimeMillis();
			log.info("BlockingCalls " + numCalls + " in " + (endTS-startTS)/1000 + "s");
		} catch( Throwable t ) {
			log.warn(t);
		} finally {
			System.exit(0);
		}
    }
}
