/**
 *   Copyright 2010-2014 Peter Klauser
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
*/
package com.googlecode.protobuf.pro.duplex.example.simple;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientPipelineFactory;
import com.googlecode.protobuf.pro.duplex.client.RpcClientConnectionWatchdog;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.ExtendedPing;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.ExtendedPong;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;

public class SimpleClient {

	private static Logger log = LoggerFactory.getLogger(SimpleClient.class);

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

		try {
			DuplexTcpClientPipelineFactory clientFactory = new DuplexTcpClientPipelineFactory();
			// force the use of a local port
			// - normally you don't need this
			clientFactory.setClientInfo(client);
			
	    	ExtensionRegistry r = ExtensionRegistry.newInstance();
			PingPong.registerAllExtensions(r);
			clientFactory.setExtensionRegistry(r);

			clientFactory.setConnectResponseTimeoutMillis(10000);
	    	RpcServerCallExecutor rpcExecutor = new ThreadPoolCallExecutor(3, 10);
			clientFactory.setRpcServerCallExecutor(rpcExecutor);			
			
			// RPC payloads are uncompressed when logged - so reduce logging
			CategoryPerServiceLogger logger = new CategoryPerServiceLogger();
			logger.setLogRequestProto(false);
			logger.setLogResponseProto(false);
			clientFactory.setRpcLogger(logger);
			
			// Set up the event pipeline factory.
	        // setup a RPC event listener - it just logs what happens
	        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
	        
	        final RpcConnectionEventListener listener = new RpcConnectionEventListener() {
				
				@Override
				public void connectionReestablished(RpcClientChannel clientChannel) {
					log.info("connectionReestablished " + clientChannel);
					channel = clientChannel;
				}
				
				@Override
				public void connectionOpened(RpcClientChannel clientChannel) {
					log.info("connectionOpened " + clientChannel);
					channel = clientChannel;
				}
				
				@Override
				public void connectionLost(RpcClientChannel clientChannel) {
					log.info("connectionLost " + clientChannel);
				}
				
				@Override
				public void connectionChanged(RpcClientChannel clientChannel) {
					log.info("connectionChanged " + clientChannel);
					channel = clientChannel;
				}
			};
			rpcEventNotifier.addEventListener(listener);
			clientFactory.registerConnectionEventListener(rpcEventNotifier);

			Bootstrap bootstrap = new Bootstrap();
	        EventLoopGroup workers = new NioEventLoopGroup(16,new RenamingThreadFactoryProxy("workers", Executors.defaultThreadFactory()));

	        bootstrap.group(workers);
	        bootstrap.handler(clientFactory);
	        bootstrap.channel(NioSocketChannel.class);
	        bootstrap.option(ChannelOption.TCP_NODELAY, true);
	    	bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,10000);
	        bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
	        bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);

			RpcClientConnectionWatchdog watchdog = new RpcClientConnectionWatchdog(clientFactory,bootstrap);
			rpcEventNotifier.addEventListener(watchdog);
	        watchdog.start();

			CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
			shutdownHandler.addResource(workers);
			shutdownHandler.addResource(rpcExecutor);
			
	        clientFactory.peerWith(server, bootstrap);
			
			while (true && channel != null) {
				
				BlockingPingService.BlockingInterface blockingService = BlockingPingService.newBlockingStub(channel);
				final ClientRpcController controller = channel.newRpcController();
				controller.setTimeoutMs(0);
					
				Ping.Builder pingBuilder = Ping.newBuilder();
				pingBuilder.setSequenceNo(1);
				pingBuilder.setPingDurationMs(1000);
				pingBuilder.setPingPayload(ByteString.copyFromUtf8("Hello World!"));
				pingBuilder.setPingPercentComplete(false);
				pingBuilder.setPongRequired(false);
				pingBuilder.setPongBlocking(true);
				pingBuilder.setPongDurationMs(1000);
				pingBuilder.setPongTimeoutMs(0);
				pingBuilder.setPongPercentComplete(false);

				// set an extension value
				pingBuilder.setExtension(ExtendedPing.extendedIntField, 111);
				
				Ping ping = pingBuilder.build();
				try {
					Pong pong = blockingService.ping(controller, ping);
					
					Integer ext = pong.getExtension(ExtendedPong.extendedIntField);
					if ( ext == null || ext != 111) {
						log.warn("Extension not parsed. Value=", ext);
					}
				} catch ( ServiceException e ) {
					log.warn("Call failed.", e);
				}
				
				Thread.sleep(10000);
				
			}

		} catch ( Exception e ) {
			log.warn("Failure.", e);
		} finally {
			System.exit(0);
		}
	}

}
