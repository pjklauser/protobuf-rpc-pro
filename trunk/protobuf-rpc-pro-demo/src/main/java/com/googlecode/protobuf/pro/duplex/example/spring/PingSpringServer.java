package com.googlecode.protobuf.pro.duplex.example.spring;

import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.protobuf.Service;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.example.PingPongServiceFactory;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPingService;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;

public class PingSpringServer {
	
	@Autowired(required = true)
	private PingPongServiceFactory.NonBlockingPingServer pingPongServiceImpl;

	int port;
	String host;

	protected final Log log = LogFactory.getLog(getClass());

	private DuplexTcpServerBootstrap bootstrap;

	public PingSpringServer(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@PostConstruct
	public void init() {
		runServer();
	}

	public void runServer() {
		// SERVER
		PeerInfo serverInfo = new PeerInfo(host, port);
		RpcServerCallExecutor executor = new ThreadPoolCallExecutor(10, 10);

		bootstrap = new DuplexTcpServerBootstrap(serverInfo,
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));
		bootstrap.setRpcServerCallExecutor(executor);
		log.info("Proto Serverbootstrap created");
		
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

		// Register Ping Service
		Service pingService = NonBlockingPingService.newReflectiveService(pingPongServiceImpl);

		bootstrap.getRpcServiceRegistry().registerService(pingService);
		log.info("Proto Ping Registerservice executed");

		bootstrap.bind();
		log.info("Proto Ping Server Bound to port " + port);
	}

	@PreDestroy
	protected void unbind() throws Throwable {
		super.finalize();
		bootstrap.releaseExternalResources();
		log.info("Proto Ping Server Unbound");
	}
}