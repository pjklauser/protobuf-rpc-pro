package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.test.PingPong.PingPongService;
import com.googlecode.protobuf.pro.duplex.test.PingPong.PingPongService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Pong;

public class DuplexPingPongClient {

	private static Log log = LogFactory.getLog(RpcClient.class);
	
    public static void main(String[] args) throws Exception {
		if ( args.length != 4 ) {
			System.err.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);
		
		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);
    	
		RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 10);
		
    	DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
        		client, 
        		new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()),
                executor);
        
		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
		shutdownHandler.addResource(bootstrap);
		
    	bootstrap.getRpcServiceRegistry().registerService(new PingPongServiceImpl());
    	
        // Set up the event pipeline factory.
    	bootstrap.setOption("connectTimeoutMillis",10000);
        bootstrap.setOption("connectResponseTimeoutMillis",10000);
        bootstrap.setOption("sendBufferSize", 1048576);
        bootstrap.setOption("receiveBufferSize", 1048576);
        bootstrap.setOption("tcpNoDelay", false);

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
        
    	try {
	        RpcClient rpcClient = bootstrap.peerWith(server);
	        
			BlockingInterface myService = PingPongService.newBlockingStub(rpcClient);
			
			RpcController controller = rpcClient.newRpcController();
			Ping request = Ping.newBuilder().setPingData(ByteString.copyFromUtf8("PingClient")).setProcessingTime(10).setPongDataLength(100).build();
			Pong response = myService.ping(controller, request);
			
			log.info("Response received " + response.toString());
    	} finally {
    		System.exit(0);
    	}
    }

	static class PingPongServiceImpl extends PingPongService {

		@Override
		public void ping(RpcController controller, Ping request,
				RpcCallback<Pong> done) {
			Pong response = Pong.newBuilder().setPongData(ByteString.copyFromUtf8("Client Result")).build();
			done.run(response);
		}

		@Override
		public void fail(RpcController controller, Ping request,
				RpcCallback<Pong> done) {
			
			controller.setFailed("Failed.");
			done.run(null);
		}
	}
}