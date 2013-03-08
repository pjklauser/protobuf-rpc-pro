/**
 *   Copyright 2010-2013 Peter Klauser
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

import java.util.concurrent.Executors;

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Service;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.example.program.AllClientTests;
import com.googlecode.protobuf.pro.duplex.example.program.ClientPerformanceTests;
import com.googlecode.protobuf.pro.duplex.example.program.ShortTests;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPongService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPongService;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutExecutor;

public class DuplexPingPongClient {

	private static Logger log = LoggerFactory.getLogger(RpcClient.class);
	
    public static void main(String[] args) throws Exception {
		if ( args.length != 8 ) {
			System.err.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort> <ssl=Y/N> <nodelay=Y/N> <compress=Y/N> <payloadSizeBytes>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);
		boolean secure = "Y".equals(args[4]);
		boolean nodelay = "Y".equals(args[5]);
		boolean compress = "Y".equals(args[6]);
		int payloadSize = Integer.parseInt(args[7]);

		log.info("DuplexPingPongClient port=" + clientPort  +" ssl=" + (secure?"Y":"N") + " nodelay=" + (nodelay?"Y":"N")+ " payloadSizeBytes="+payloadSize);
		
		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);
    	
		RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 100 );
		
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

        // Configure the client to provide a Pong Service in both blocking an non blocking varieties
       	BlockingService bPongService = BlockingPongService.newReflectiveBlockingService(new PingPongServiceFactory.BlockingPongServer());
       	bootstrap.getRpcServiceRegistry().registerBlockingService(bPongService);

       	Service nbPongService = NonBlockingPongService.newReflectiveService(new PingPongServiceFactory.NonBlockingPongServer());
        bootstrap.getRpcServiceRegistry().registerService(nbPongService);
    	
        // we give the client a blocking and non blocking (pong capable) Ping Service
        BlockingService bPingService = BlockingPingService.newReflectiveBlockingService(new PingPongServiceFactory.BlockingPongingPingServer());
        bootstrap.getRpcServiceRegistry().registerBlockingService(bPingService);

        Service nbPingService = NonBlockingPingService.newReflectiveService(new PingPongServiceFactory.NonBlockingPongingPingServer());
        bootstrap.getRpcServiceRegistry().registerService(nbPingService);

    	bootstrap.peerWith(server);
    	try {
    		
    		while( true ) {
    			
    			new ClientPerformanceTests().execute(bootstrap.getRpcClientRegistry());
    			
    	    	new ShortTests().execute(bootstrap.getRpcClientRegistry());
    	    	
    	    	new AllClientTests().execute(bootstrap.getRpcClientRegistry());
    	    	
    	    	Thread.sleep(60000);
    		}
		} catch( Throwable e ) {
			log.error("Throwable.", e);
		} finally {
			System.exit(0);
		}
    }
    
}
