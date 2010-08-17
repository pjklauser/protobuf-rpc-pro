package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.execute.ServerRpcController;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.googlecode.protobuf.pro.duplex.server.RpcClientConnectionRegistry;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.test.PingPong.PingPongService;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Pong;

public class DuplexPingPongServer {

	private static Log log = LogFactory.getLog(RpcClient.class);

    public static void main(String[] args) throws Exception {
    	PeerInfo serverInfo = new PeerInfo("server'sHostname", 8080);
    	
//    	SameThreadExecutor executor = new SameThreadExecutor();
    	ThreadPoolCallExecutor executor = new ThreadPoolCallExecutor(3, 10);
    	
        // Configure the server.
        DuplexTcpServerBootstrap bootstrap = new DuplexTcpServerBootstrap(
        		serverInfo,
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()),
                executor);

        bootstrap.setOption("sendBufferSize", 1048576);
        bootstrap.setOption("receiveBufferSize", 1048576);
        bootstrap.setOption("child.receiveBufferSize", 1048576);
        bootstrap.setOption("child.sendBufferSize", 1048576);
        bootstrap.setOption("tcpNoDelay", false);
        
    	RpcClientConnectionRegistry eventLogger = new RpcClientConnectionRegistry();
    	bootstrap.registerConnectionEventListener(eventLogger);

    	bootstrap.getRpcServiceRegistry().registerService(new PingPongServiceImpl());
    	
    	// Bind and start to accept incoming connections.
        Channel c = bootstrap.bind();
        
        System.out.println("Bound to " + c.getLocalAddress());
        /*
        Thread.sleep(10000);
        c.close();
        bootstrap.releaseExternalResources();
        */
    }
    
	static class PingPongServiceImpl extends PingPongService {

		@Override
		public void ping(RpcController controller, Ping request,
				RpcCallback<Pong> done) {
			
			
			RpcClientChannel channel = ServerRpcController.getRpcChannel(controller);
			BlockingInterface myService = PingPongService.newBlockingStub(channel);
			RpcController clientController = channel.newRpcController();

			byte[] clientResult = new byte[0];
			try {
				Ping clientRequest = Ping.newBuilder().setProcessingTime(100).setPingData(ByteString.copyFromUtf8("ClientPing")).setPongDataLength(100).build();

				Pong clientResponse = myService.ping(clientController, clientRequest);
				clientResult = clientResponse.getPongData().toByteArray();
				
			} catch ( ServiceException e ) {
				controller.setFailed("Client call failed with " + e.getMessage());
				done.run(null);
				return;
			}
			/*
			try {
				Thread.sleep(1000);
			} catch ( InterruptedException e ) {
				Thread.currentThread().interrupt();
			}
			*/
			Pong response = Pong.newBuilder().setPongData(ByteString.copyFrom(clientResult)).build();
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
