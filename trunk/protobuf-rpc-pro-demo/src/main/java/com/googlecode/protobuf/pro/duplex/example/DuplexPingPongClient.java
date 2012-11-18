package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.BlockingService;
import com.google.protobuf.ByteString;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClient.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutExecutor;

public class DuplexPingPongClient {

	private static Log log = LogFactory.getLog(RpcClient.class);
	
    public static void main(String[] args) throws Exception {
		if ( args.length != 12 ) {
			System.err.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort> <blocking=Y/N> <ssl=Y/N> <nodelay=Y/N> <compress=Y/N> <payloadBytes> <numCalls> <pingDurationTimeMs> <pingTimeoutMs>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);

		boolean blocking = args[4].equalsIgnoreCase("Y");
		boolean secure = args[5].equalsIgnoreCase("Y");
		boolean nodelay = args[6].equalsIgnoreCase("Y");
		
		boolean compress = "Y".equals(args[7]);
		int payloadSize = Integer.parseInt(args[8]);

		int numCalls = Integer.parseInt(args[9]);
		int pingDurationMs = Integer.parseInt(args[10]); // sent to the server to determine how long the server takes to process the ping.
		int pingTimeoutMs = Integer.parseInt(args[11]);
		
		log.info("DuplexPingPongClient port=" + clientPort  +" blocking=" + (blocking?"Y":"N")+ " ssl=" + (secure?"Y":"N") + " nodelay=" + (nodelay?"Y":"N")+ " pingDurationMs="+pingDurationMs +" pingTimeoutMs="+pingTimeoutMs);
		
		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);
    	
		RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 10 );
		
    	DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
        		client, 
        		new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));
    	bootstrap.setRpcServerCallExecutor(executor);
        bootstrap.setCompression(compress);
        if ( secure ) {
        	RpcSSLContext sslCtx = new RpcSSLContext();
        	sslCtx.setKeystorePassword("changeme");
        	sslCtx.setKeystorePath("./lib/client.keystore");
        	sslCtx.setTruststorePassword("changeme");
        	sslCtx.setTruststorePath("./lib/truststore");
        	sslCtx.init();
        }
        
        // Configure the client.
        if ( blocking ) {
        	BlockingService pongService = PongService.newReflectiveBlockingService(new PingPongServiceFactory.BlockingPongService());
        	bootstrap.getRpcServiceRegistry().registerBlockingService(pongService);
        } else {
        	Service pongService = PongService.newReflectiveService(new PingPongServiceFactory.NonBlockingPongServer());
        	bootstrap.getRpcServiceRegistry().registerService(pongService);
        }

        // Set up the event pipeline factory.
    	bootstrap.setOption("connectTimeoutMillis",10000);
        bootstrap.setOption("connectResponseTimeoutMillis",10000);
        bootstrap.setOption("sendBufferSize", 1048576);
        bootstrap.setOption("receiveBufferSize", 1048576);
        bootstrap.setOption("tcpNoDelay", nodelay);

        RpcTimeoutExecutor timeoutExecutor = new TimeoutExecutor(1,5);
		RpcTimeoutChecker checker = new TimeoutChecker();
		checker.setTimeoutExecutor(timeoutExecutor);
		checker.startChecking(bootstrap.getRpcClientRegistry());

        CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(bootstrap);
        shutdownHandler.addResource(executor);
        shutdownHandler.addResource(checker);
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
        
    	RpcClientChannel channel = null;
		try {
	    	channel = bootstrap.peerWith(server);
			BlockingInterface myService = PingService.newBlockingStub(channel);
			
	    	long startTS = 0;
	    	long endTS = 0;
	    	long numException = 0;
	    	
			startTS = System.currentTimeMillis();

			for( int i = 0; i < numCalls; i++ ) {
				if ( i % 100 == 1 ) {
					log.warn(i);
				}
				ClientRpcController controller = channel.newRpcController();
				controller.setTimeoutMs(pingTimeoutMs);
				
				ByteString requestData = ByteString.copyFrom(new byte[payloadSize]);
				Ping ping = Ping.newBuilder().setNumber(pingDurationMs).setPingData(requestData).build();
				try {
					Pong pong = myService.ping(controller, ping);
					if ( pong.getPongData().size() != payloadSize ) {
						throw new Exception("Reply payload mismatch.");
					}
				} catch( ServiceException e ) {
					log.warn("Ping ServiceException.", e);
					numException++;
				} 
			}
			endTS = System.currentTimeMillis();
			log.warn("Calls " + numCalls + " in " + (endTS-startTS)/1000 + "s with " + numException + " ServiceExceptions.");
		} catch( Throwable e ) {
			log.error("Throwable.", e);
		} finally {
			System.exit(0);
		}
    }
}
