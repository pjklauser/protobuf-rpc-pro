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
package com.googlecode.protobuf.pro.duplex.example;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Service;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.example.program.ShortTests;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPongService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPongService;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerPipelineFactory;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;

public class DuplexPingPongServer {

	private static Logger log = LoggerFactory.getLogger(DuplexPingPongServer.class);

    public static void main(String[] args) throws Exception {
		if ( args.length < 4 ) {
			System.err.println("usage: <serverHostname> <serverPort> <ssl=Y/N> <nodelay=Y/N>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		boolean secure = "Y".equals(args[2]);
		boolean nodelay = "Y".equals(args[3]);
		long runDuration = 0;
		if ( args.length > 4) {
			runDuration = Long.parseLong(args[4]);
		}
		
		log.info("DuplexPingPongServer " + serverHostname + ":"+ serverPort +" ssl=" + (secure?"Y":"N") + " nodelay=" + (nodelay?"Y":"N"));
		
    	PeerInfo serverInfo = new PeerInfo(serverHostname, serverPort);

    	RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 200);
    	
    	DuplexTcpServerPipelineFactory serverFactory = new DuplexTcpServerPipelineFactory(serverInfo);
    	serverFactory.setRpcServerCallExecutor(executor);
        if ( secure ) {
        	RpcSSLContext sslCtx = new RpcSSLContext();
        	sslCtx.setKeystorePassword("changeme");
        	sslCtx.setKeystorePath("./lib/server.keystore");
        	sslCtx.setTruststorePassword("changeme");
        	sslCtx.setTruststorePath("./lib/truststore");
        	sslCtx.init();
        	
        	serverFactory.setSslContext(sslCtx);
        }
        
        RpcTimeoutExecutor timeoutExecutor = new TimeoutExecutor(1,5);
		RpcTimeoutChecker timeoutChecker = new TimeoutChecker();
		timeoutChecker.setTimeoutExecutor(timeoutExecutor);
		timeoutChecker.startChecking(serverFactory.getRpcClientRegistry());

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
		serverFactory.registerConnectionEventListener(rpcEventNotifier);

    	// we give the server a blocking and non blocking (pong capable) Ping Service
        BlockingService bPingService = BlockingPingService.newReflectiveBlockingService(new PingPongServiceFactory.BlockingPongingPingServer());
        serverFactory.getRpcServiceRegistry().registerService(bPingService);

        Service nbPingService = NonBlockingPingService.newReflectiveService(new PingPongServiceFactory.NonBlockingPongingPingServer());
        serverFactory.getRpcServiceRegistry().registerService(nbPingService);

        // Configure the server to provide a Pong Service in both blocking an non blocking varieties
       	BlockingService bPongService = BlockingPongService.newReflectiveBlockingService(new PingPongServiceFactory.BlockingPongServer());
       	serverFactory.getRpcServiceRegistry().registerService(bPongService);

       	Service nbPongService = NonBlockingPongService.newReflectiveService(new PingPongServiceFactory.NonBlockingPongServer());
       	serverFactory.getRpcServiceRegistry().registerService(nbPongService);
        
        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap();
        NioEventLoopGroup boss = new NioEventLoopGroup(2,new RenamingThreadFactoryProxy("boss", Executors.defaultThreadFactory()));
        NioEventLoopGroup workers = new NioEventLoopGroup(16,new RenamingThreadFactoryProxy("worker", Executors.defaultThreadFactory()));
        bootstrap.group(boss, workers);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);
        bootstrap.childOption(ChannelOption.SO_RCVBUF, 1048576);
        bootstrap.childOption(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.TCP_NODELAY, nodelay);
        bootstrap.childHandler(serverFactory);
        bootstrap.localAddress(serverInfo.getPort());
        
    	// Bind and start to accept incoming connections.
		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(boss);
        shutdownHandler.addResource(workers);
        shutdownHandler.addResource(executor);
        shutdownHandler.addResource(timeoutChecker);
        shutdownHandler.addResource(timeoutExecutor);
        
        bootstrap.bind();
        
        log.info("Serving " + serverInfo);

        if ( runDuration > 0 ) {
        	Thread.sleep(runDuration);   
        	System.exit(0);
        } else {
        	while( true ) {
        		try {
        			log.info("Sleeping 60s before retesting clients.");
        	    	Thread.sleep(60000);
        	    	new ShortTests().execute(serverFactory.getRpcClientRegistry());
        		} catch( Throwable e ) {
        			log.warn("Throwable.", e);
        		}
        	}
        }
    }
    
    
}
