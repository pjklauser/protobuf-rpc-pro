package com.googlecode.protobuf.pro.duplex.example;

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
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;

public class PingClient {

	private static Log log = LogFactory.getLog(PingClient.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 8) {
			System.err
					.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort> <numCalls> <processingTimeMs> <payloadBytes> <compressY/N>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);

		int numCalls = Integer.parseInt(args[4]);
		int procTime = Integer.parseInt(args[5]);
		int payloadSize = Integer.parseInt(args[6]);
		boolean compress = "Y".equals(args[7]);

		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);

		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
		try {
			DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
					client, new NioClientSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool()),
					new ThreadPoolCallExecutor(3, 10));
			bootstrap.setCompression(compress);
			
			// RPC payloads are uncompressed when logged - so reduce logging
			CategoryPerServiceLogger logger = new CategoryPerServiceLogger();
			logger.setLogRequestProto(false);
			logger.setLogResponseProto(false);
			bootstrap.setRpcLogger(logger);
			
			shutdownHandler.addResource(bootstrap);

			// Set up the event pipeline factory.
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

			RpcClientChannel channel = bootstrap.peerWith(server);

			BlockingInterface myService = PingService.newBlockingStub(channel);

			long startTS = 0;
			long endTS = 0;

			startTS = System.currentTimeMillis();
			for (int i = 0; i < numCalls; i++) {
				if (i % 1000 == 1) {
					System.out.println(i);
				}
				RpcController controller = channel.newRpcController();

				ByteString requestData = ByteString.copyFrom(new byte[payloadSize]);
				Ping request = Ping.newBuilder().setNumber(procTime)
						.setPingData(requestData).build();
				Pong pong = myService.ping(controller, request);
				if (pong.getPongData().size() != payloadSize) {
					throw new Exception("Reply payload mismatch.");
				}
			}
			endTS = System.currentTimeMillis();
			log.error("BlockingCalls " + numCalls + " in " + (endTS - startTS)
					/ 1000 + "s");

		} finally {
			System.exit(0);
		}
	}

}