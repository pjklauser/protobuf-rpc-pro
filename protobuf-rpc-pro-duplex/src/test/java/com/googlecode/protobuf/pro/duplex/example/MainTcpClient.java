package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.client.RpcServerConnectionRegistry;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.test.PingPong.PingPongService;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.test.PingPong.PingPongService.BlockingInterface;

public class MainTcpClient {
	
	private static Log log = LogFactory.getLog(MainTcpClient.class);
	
	public static void main(String[] args) throws Exception {
		PeerInfo client = new PeerInfo("clienthost", 8081);
    	
//    	SameThreadExecutor executor = new SameThreadExecutor();
    	ThreadPoolCallExecutor executor = new ThreadPoolCallExecutor(3, 10);

    	DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
        		client, 
        		new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()),
                executor);
        
        // Configure the client.

        // Set up the event pipeline factory.
    	RpcServerConnectionRegistry eventLogger = new RpcServerConnectionRegistry();
    	bootstrap.registerConnectionEventListener(eventLogger);
        
    	RpcClientChannel channel = null;
		try {
	    	channel = bootstrap.peerWith("localhost", 8080);
	    	long startTS = 0;
	    	long endTS = 0;
			int numCalls = 10000;
	    	
			startTS = System.currentTimeMillis();
			try {
				nonBlockingCalls(numCalls, 0, 10, 10, channel);
			} catch ( Exception e ) {
				log.warn("NonBlockingCalls failed. ", e);
			}
			endTS = System.currentTimeMillis();
			log.info("NonBlockingCalls " + numCalls + " in " + (endTS-startTS)/1000 + "s");
			
			Thread.sleep(1000); // we will have overloaded the server a bit - let it recover.
				
			startTS = System.currentTimeMillis();
			try {
				blockingCalls(numCalls, 0, 1, 1, channel);
			} catch ( Exception e ) {
				log.warn("BlockingCalls failed. ", e);
			}
			endTS = System.currentTimeMillis();
			log.info("BlockingCalls " + numCalls + " in " + (endTS-startTS)/1000 + "s");
			
			
			// here we send 10 long running calls to the server and cancel them
			nonBlockingCallCancellation(10, channel);
			
			// here we close a client channel when there are nonblocking calls in progress
			// on the server side. The calls should be "Cancelled on Close" on the server side
			// and "Forced Closure" on the client side.
			nonBlockingCallClose(10,channel);
		} finally {
			if ( channel != null ) {
				channel.close();
			}
			
			Thread.sleep(1000);
			
			bootstrap.releaseExternalResources();
		}
	}
	
	private static void nonBlockingCallClose(int numCalls, RpcClientChannel channel) throws Exception {
		final RpcController[] controllerList = new RpcController[numCalls];
		final Pong[] responseList = new Pong[numCalls];
		final boolean[] finishedList = new boolean[numCalls];
		
		PingPongService myService = PingPongService.newStub(channel);

		for( int i = 0; i < numCalls; i++ ) {
			RpcController controller = channel.newRpcController();
			controllerList[i] = controller;
			PongRpcCallback done = new PongRpcCallback(i) {

				/* (non-Javadoc)
				 * @see com.googlecode.protobuf.pro.test.PongRpcCallback#run(com.googlecode.protobuf.pro.test.PingPong.Pong)
				 */
				@Override
				public void run(Pong responseMessage) {
					responseList[getPos()] = responseMessage;
					finishedList[getPos()] = true;
				}
				
			};
			
			ByteString requestData = ByteString.copyFrom(new byte[10]);
			Ping request = Ping.newBuilder().setProcessingTime(10000).setPingData(requestData).setPongDataLength(10).build();
			myService.ping(controller, request, done);
		}
		
		Thread.sleep(1000);
		
		channel.close();
		
		boolean finished = false;
		int sleepCount = 0;
		while( !finished ) {
			Thread.sleep(1000);
			finished = true;
			for( boolean a : finishedList ) {
				finished &= a;
			}
			sleepCount++;
			if ( sleepCount > 20 ) {
				throw new Exception("Calls not finished within " + sleepCount + " seconds for after close.");
			}
		}
		for( int i = 0; i < numCalls; i++ ) {
			RpcController ctr = controllerList[i];
			if ( ctr.failed() ) {
				System.out.println("RpcCall["+i+"] failed with reason: " + ctr.errorText());
				
				if ( !"Forced Closure".equals(ctr.errorText() )) {
					throw new Exception(ctr.errorText()); 
				}
			}
		}
	}
	
	private static void nonBlockingCallCancellation(int numCalls, RpcClientChannel channel) throws Exception {
		final RpcController[] controllerList = new RpcController[numCalls];
		final Pong[] responseList = new Pong[numCalls];
		final boolean[] finishedList = new boolean[numCalls];
		
		PingPongService myService = PingPongService.newStub(channel);

		for( int i = 0; i < numCalls; i++ ) {
			RpcController controller = channel.newRpcController();
			controllerList[i] = controller;
			PongRpcCallback done = new PongRpcCallback(i) {

				/* (non-Javadoc)
				 * @see com.googlecode.protobuf.pro.test.PongRpcCallback#run(com.googlecode.protobuf.pro.test.PingPong.Pong)
				 */
				@Override
				public void run(Pong responseMessage) {
					responseList[getPos()] = responseMessage;
					finishedList[getPos()] = true;
				}
				
			};
			
			ByteString requestData = ByteString.copyFrom(new byte[10]);
			Ping request = Ping.newBuilder().setProcessingTime(10000).setPingData(requestData).setPongDataLength(10).build();
			myService.ping(controller, request, done);
		}
		
		for( int i = 0; i < numCalls; i++ ) {
			RpcController controller = controllerList[i];
			controller.startCancel();
		}
		
		boolean finished = false;
		int sleepCount = 0;
		while( !finished ) {
			Thread.sleep(1000);
			finished = true;
			for( boolean a : finishedList ) {
				finished &= a;
			}
			sleepCount++;
			if ( sleepCount > 20 ) {
				throw new Exception("Cancellation calls not finished within " + sleepCount + " seconds for " + numCalls + " cancelled calls.");
			}
		}
		for( int i = 0; i < numCalls; i++ ) {
			RpcController ctr = controllerList[i];
			if ( ctr.failed() ) {
				System.out.println("RpcCall["+i+"] failed with reason: " + ctr.errorText());
				
				if ( !"Cancel".equals(ctr.errorText() )) {
					throw new Exception(ctr.errorText()); 
				}
			}
		}
	}
	
	private static void nonBlockingCalls(int numCalls, int processingTimeMs, int payloadSize, int replyPayloadSize, RpcClientChannel channel) throws Exception {
		final RpcController[] controllerList = new RpcController[numCalls];;
		final Pong[] responseList = new Pong[numCalls];
		final boolean[] finishedList = new boolean[numCalls];
		
		PingPongService myService = PingPongService.newStub(channel);

		for( int i = 0; i < numCalls; i++ ) {
			RpcController controller = channel.newRpcController();
			controllerList[i] = controller;
			PongRpcCallback done = new PongRpcCallback(i) {

				/* (non-Javadoc)
				 * @see com.googlecode.protobuf.pro.test.PongRpcCallback#run(com.googlecode.protobuf.pro.test.PingPong.Pong)
				 */
				@Override
				public void run(Pong responseMessage) {
					responseList[getPos()] = responseMessage;
					finishedList[getPos()] = true;
				}
				
			};
			
			ByteString requestData = ByteString.copyFrom(new byte[payloadSize]);
			Ping request = Ping.newBuilder().setProcessingTime(processingTimeMs).setPingData(requestData).setPongDataLength(replyPayloadSize).build();
			myService.ping(controller, request, done);
		}
		
		boolean finished = false;
		int sleepCount = 0;
		while( !finished ) {
			Thread.sleep(1000);
			finished = true;
			for( boolean a : finishedList ) {
				finished &= a;
			}
			sleepCount++;
			if ( sleepCount > numCalls / 100 ) {
				throw new Exception("Non blocking calls not finished within " + sleepCount + " seconds for " + numCalls + " calls.");
			}
		}
		for( int i = 0; i < numCalls; i++ ) {
			RpcController ctr = controllerList[i];
			if ( ctr.failed() ) {
				System.out.println("RpcCall["+i+"] failed with reason: " + ctr.errorText());
				
				// we expect to be able to overload the server when we blast it with too many non
				// blocking calls.
				if ( !"Server Overload".equals(ctr.errorText() )) {
					throw new Exception(ctr.errorText()); 
				}
			}
		}
	}
	
	private static void blockingCalls(int numCalls, int processingTime, int payloadSize, int replyPayloadSize, RpcClientChannel channel) throws Exception {
		BlockingInterface myService = PingPongService.newBlockingStub(channel);

		for( int i = 0; i < numCalls; i++ ) {
			if ( i % 100 == 1 ) {
				System.out.println(i);
			}
			RpcController controller = channel.newRpcController();
			
			ByteString requestData = ByteString.copyFrom(new byte[payloadSize]);
			Ping request = Ping.newBuilder().setProcessingTime(processingTime).setPingData(requestData).setPongDataLength(replyPayloadSize).build();
			Pong pong = myService.ping(controller, request);
			if ( pong.getPongData().size() != replyPayloadSize ) {
				throw new Exception("Reply payload mismatch.");
			}
		}
	}
	
	
}
