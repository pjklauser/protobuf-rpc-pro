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
package com.googlecode.protobuf.pro.duplex.example.nonrpc;

import java.util.concurrent.Executors;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientPipelineFactory;
import com.googlecode.protobuf.pro.duplex.client.RpcClientConnectionWatchdog;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;

public class StatusClient {

	private static Logger log = LoggerFactory.getLogger(StatusClient.class);

	private static RpcClientChannel channel = null;
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("usage: <serverHostname> <serverPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);

		PeerInfo server = new PeerInfo(serverHostname, serverPort);

		try {
			DuplexTcpClientPipelineFactory clientFactory = new DuplexTcpClientPipelineFactory();
			clientFactory.setConnectResponseTimeoutMillis(10000);
			clientFactory.setRpcServerCallExecutor(new ThreadPoolCallExecutor(3, 10));			
			
			// RPC payloads are uncompressed when logged - so reduce logging
			CategoryPerServiceLogger logger = new CategoryPerServiceLogger();
			logger.setLogRequestProto(false);
			logger.setLogResponseProto(false);
			clientFactory.setRpcLogger(logger);
			
			final RpcCallback<PingPong.Status> serverStatusCallback = new RpcCallback<PingPong.Status>() {

				@Override
				public void run(PingPong.Status parameter) {
					log.info("Received " + parameter);
				}
	        	
			};
			
			// Set up the event pipeline factory.
	        // setup a RPC event listener - it just logs what happens
	        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
	        
	        final RpcConnectionEventListener listener = new RpcConnectionEventListener() {
				
				@Override
				public void connectionReestablished(RpcClientChannel clientChannel) {
					log.info("connectionReestablished " + clientChannel);
					channel = clientChannel;
					channel.setOobMessageCallback(PingPong.Status.getDefaultInstance(), serverStatusCallback);
				}
				
				@Override
				public void connectionOpened(RpcClientChannel clientChannel) {
					log.info("connectionOpened " + clientChannel);
					channel = clientChannel;
					clientChannel.setOobMessageCallback(PingPong.Status.getDefaultInstance(), serverStatusCallback);
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

	        clientFactory.peerWith(server, bootstrap);
			
			while (true && channel != null) {
				
            	PingPong.Status clientStatus = PingPong.Status.newBuilder()
            			.setMessage("Client " + channel + " OK@" + System.currentTimeMillis()).build();
            	
            	ChannelFuture oobSend = channel.sendOobMessage(clientStatus);
            	if ( !oobSend.isDone() ) {
            		log.info("Waiting for completion.");
            		oobSend.syncUninterruptibly();
            	}
            	if ( !oobSend.isSuccess() ) {
            		log.warn("OobMessage send failed.", oobSend.cause());
            	}
				
				Thread.sleep(1000);
				
			}

		} finally {
			System.exit(0);
		}
	}

}
