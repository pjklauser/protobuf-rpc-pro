package com.googlecode.protobuf.pro.duplex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.logging.NullLogger;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerPipelineFactory;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;

public class ShutdownHandlerTest {
	private class Wrapper {
		public CleanShutdownHandler shutdownHandler;
		public NioEventLoopGroup boss;
		public NioEventLoopGroup workers;
		public RpcServerCallExecutor executor;
		public RpcTimeoutChecker timeoutChecker;
		public RpcTimeoutExecutor timeoutExecutor;
	}
	
	private Wrapper createFakeServer() {
		Wrapper w = new Wrapper();
		
		w.executor = new ThreadPoolCallExecutor(3, 200);
		DuplexTcpServerPipelineFactory connectionFactory = new DuplexTcpServerPipelineFactory(new PeerInfo("fake", 1));
		connectionFactory.setRpcServerCallExecutor(w.executor);
		connectionFactory.setLogger(new NullLogger());

		w.timeoutExecutor = new TimeoutExecutor(1, 5);
		w.timeoutChecker = new TimeoutChecker();
		w.timeoutChecker.setTimeoutExecutor(w.timeoutExecutor);
		w.timeoutChecker.startChecking(connectionFactory.getRpcClientRegistry());

		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap();
		w.boss = new NioEventLoopGroup(2, new RenamingThreadFactoryProxy("boss", Executors.defaultThreadFactory()));
		w.workers = new NioEventLoopGroup(16, new RenamingThreadFactoryProxy("worker", Executors.defaultThreadFactory()));
		bootstrap.group(w.boss, w.workers);
		bootstrap.channel(NioServerSocketChannel.class);
		bootstrap.childHandler(connectionFactory);

		// Bind and start to accept incoming connections.
		w.shutdownHandler = new CleanShutdownHandler();
		w.shutdownHandler.addResource(w.boss);
		w.shutdownHandler.addResource(w.workers);
		w.shutdownHandler.addResource(w.executor);
		w.shutdownHandler.addResource(w.timeoutChecker);
		w.shutdownHandler.addResource(w.timeoutExecutor);
		
		return w;
	}
	
	private boolean getShutdownState(Wrapper toto) {
		return toto.boss.isShutdown()
			&& toto.workers.isShutdown()
			&& toto.executor.isShutdown()
			&& toto.timeoutChecker.isShutdown()
			&& toto.timeoutExecutor.isShutdown();
	}

	@Test
	public void testShutdownImmediate() {
		Wrapper w = createFakeServer();
		
		assertFalse(getShutdownState(w));
		w.shutdownHandler.shutdown();
		
		// Manual waiting (max 20 seconds)
		long now = System.currentTimeMillis();
		while(System.currentTimeMillis() - now < 20000) {
			if (getShutdownState(w)) {
				break;
			}
		}

		assertTrue(getShutdownState(w));
	}

	@Test
	public void testShutdownAwaiting_success() throws InterruptedException, ExecutionException {
		Wrapper w = createFakeServer();

		Future<Boolean> f = w.shutdownHandler.shutdownAwaiting(5000);
		assertNotNull(f);
		
		// ENsure there is no timeout
		assertEquals(true, f.get());
	}

	@Test
	public void testShutdownAwaiting_timeout() throws InterruptedException, ExecutionException {
		Wrapper w = createFakeServer();
		
		// Create a timeout thread and add it to resources to shutdown
		ExecutorService ex = Executors.newSingleThreadExecutor();
		ex.execute(new Runnable() {
			@Override
			public void run() {
				try
				{
					// Wait for too long
					Thread.sleep(10000);
				}
				catch (InterruptedException e) {}
			}
		});
		w.shutdownHandler.addResource(ex);
		
		Future<Boolean> f = w.shutdownHandler.shutdownAwaiting(5000);
		assertNotNull(f);
		
		// Ensure it was timed out
		assertEquals(false, f.get());
	}
}
