package com.googlecode.protobuf.pro.duplex.example;

import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ServerRpcController;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;

public class DuplexPingPongServer {

	private static Log log = LogFactory.getLog(DuplexPingPongServer.class);

    public static void main(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.println("usage: <serverHostname> <serverPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		
    	PeerInfo serverInfo = new PeerInfo(serverHostname, serverPort);
    	
    	RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 10);
    	
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
        
		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(bootstrap);
        
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

    	Service pingService = PingService.newReflectiveService(new PingServiceImpl());
    	bootstrap.getRpcServiceRegistry().registerService(pingService);
    	
    	// Bind and start to accept incoming connections.
        bootstrap.bind();
        
        log.info("Serving " + serverInfo);
    }
    
    
    /**
     * This PingService services a ping() call.
     * 
     * The ping() call will reverse call pong() of
     * the client who's calling.
     *
     * The blocking pong() call contains the same data as the ping
     * call. The final response of the server will be what
     * the client replied in the pong response.
     */
	static class PingServiceImpl implements PingService.Interface {

		@Override
		public void ping(RpcController controller, Ping request,
				RpcCallback<Pong> done) {
			
			
			RpcClientChannel channel = ServerRpcController.getRpcChannel(controller);
			BlockingInterface clientService = PongService.newBlockingStub(channel);
			RpcController clientController = channel.newRpcController();

			Ping clientResponse = null;
			try {
				Pong clientRequest = Pong.newBuilder().setNumber(request.getNumber()).setPongData(request.getPingData()).build();

				clientResponse = clientService.pong(clientController, clientRequest);
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
