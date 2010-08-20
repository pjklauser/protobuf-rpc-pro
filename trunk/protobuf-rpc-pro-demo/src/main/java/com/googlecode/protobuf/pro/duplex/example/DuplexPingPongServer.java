package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.execute.ServerRpcController;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.googlecode.protobuf.pro.duplex.server.RpcClientConnectionRegistry;

public class DuplexPingPongServer {

	private static Log log = LogFactory.getLog(RpcClient.class);

    public static void main(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.println("usage: <serverHostname> <serverPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		
    	PeerInfo serverInfo = new PeerInfo(serverHostname, serverPort);
    	
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

    	Service pingService = PingService.newReflectiveService(new PingServiceImpl());
    	bootstrap.getRpcServiceRegistry().registerService(pingService);
    	
    	// Bind and start to accept incoming connections.
        Channel c = bootstrap.bind();
        
        System.out.println("Bound to " + c.getLocalAddress());
        
        //TODO shutdown thread - release
    }
    
    
	static class PingServiceImpl implements PingService.Interface {

		@Override
		public void ping(RpcController controller, Ping request,
				RpcCallback<Pong> done) {
			
			
			RpcClientChannel channel = ServerRpcController.getRpcChannel(controller);
			BlockingInterface myService = PongService.newBlockingStub(channel);
			RpcController clientController = channel.newRpcController();

			Ping clientResponse = null;
			try {
				Pong clientRequest = Pong.newBuilder().setNumber(100).setPongData(ByteString.copyFromUtf8("ClientPing")).build();

				clientResponse = myService.pong(clientController, clientRequest);
			} catch ( ServiceException e ) {
				controller.setFailed("Client call failed with " + e.getMessage());
				done.run(null);
				return;
			}
			Pong response = Pong.newBuilder().setNumber(clientResponse.getNumber()).setPongData(clientResponse.getPingData()).build();
			done.run(response);
		}
	}
}
