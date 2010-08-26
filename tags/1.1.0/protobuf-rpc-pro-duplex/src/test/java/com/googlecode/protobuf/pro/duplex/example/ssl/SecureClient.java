package com.googlecode.protobuf.pro.duplex.example.ssl;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.test.PingPong.PingPongService;
import com.googlecode.protobuf.pro.duplex.test.PingPong.PingPongService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Pong;

public class SecureClient {

	private static Log log = LogFactory.getLog(SecureClient.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);

		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);

    	RpcSSLContext sslCtx = new RpcSSLContext();
    	sslCtx.setKeystorePassword("changeme");
    	sslCtx.setKeystorePath("ssl/client.keystore");
    	sslCtx.setTruststorePassword("changeme");
    	sslCtx.setTruststorePath("ssl/truststore");
    	sslCtx.init();
    	
		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
		try {
			DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
					client, new NioClientSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool()),
					new ThreadPoolCallExecutor(3, 10));

			// secure the client bootstrap
			bootstrap.setSslContext(sslCtx);
			
			// give the bootstrap to the shutdown handler so it is shutdown cleanly.
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

			// open the connection with the server.
			for( int i = 0; i < 10; i++ ) {
				RpcClientChannel channel = bootstrap.peerWith(server);
				
				long startTS = 0;
				long endTS = 0;
				int numCalls = 1;

				startTS = System.currentTimeMillis();
				try {
					blockingCalls(numCalls, 0, 1, 1, channel);
				} catch (Exception e) {
					log.warn("BlockingCalls failed. ", e);
				}
				endTS = System.currentTimeMillis();
				log.info("BlockingCalls " + numCalls + " in " + (endTS - startTS)/ 1000 + "s");
				
				channel.close();
			}

			
		} finally {
			System.exit(0);
		}
	}

	private static void blockingCalls(int numCalls, int processingTime,
			int payloadSize, int replyPayloadSize, RpcClientChannel channel)
			throws Exception {
		BlockingInterface myService = PingPongService.newBlockingStub(channel);

		for (int i = 0; i < numCalls; i++) {
			if (i % 100 == 1) {
				System.out.println(i);
			}
			RpcController controller = channel.newRpcController();

			ByteString requestData = ByteString.copyFrom(new byte[payloadSize]);
			Ping request = Ping.newBuilder().setProcessingTime(processingTime)
					.setPingData(requestData)
					.setPongDataLength(replyPayloadSize).build();
			Pong pong = myService.ping(controller, request);
			if (pong.getPongData().size() != replyPayloadSize) {
				throw new Exception("Reply payload mismatch.");
			}
		}
	}

}
