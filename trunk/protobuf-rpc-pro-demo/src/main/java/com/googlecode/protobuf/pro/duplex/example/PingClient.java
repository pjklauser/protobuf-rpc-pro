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
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingPongService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingPongService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;

public class PingClient {
	
	private static Log log = LogFactory.getLog(PingClient.class);
	
	public static void main(String[] args) throws Exception {
		if ( args.length != 8 ) {
			System.err.println("usage: <serverHostname> <serverPort> <clientHostname> <clientPort> <numCalls> <processingTimeMs> <upSizeBytes> <downSizeBytes>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String clientHostname = args[2];
		int clientPort = Integer.parseInt(args[3]);

		int numCalls = Integer.parseInt(args[4]);
		int procTime = Integer.parseInt(args[5]);
		int upSize = Integer.parseInt(args[6]);
		int downSize = Integer.parseInt(args[7]);
		
		PeerInfo client = new PeerInfo(clientHostname, clientPort);
		PeerInfo server = new PeerInfo(serverHostname, serverPort);
    	
		DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(
        		client, 
        		new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()),
                new ThreadPoolCallExecutor(3, 10));
        
        // Set up the event pipeline factory.
    	RpcServerConnectionRegistry eventLogger = new RpcServerConnectionRegistry();
    	bootstrap.registerConnectionEventListener(eventLogger);
        
    	RpcClientChannel channel = null;
		try {
	    	channel = bootstrap.peerWith(server);
	    	long startTS = 0;
	    	long endTS = 0;
	    	
			startTS = System.currentTimeMillis();
			BlockingInterface myService = PingPongService.newBlockingStub(channel);

			for( int i = 0; i < numCalls; i++ ) {
				if ( i % 100 == 1 ) {
					System.out.println(i);
				}
				RpcController controller = channel.newRpcController();
				
				ByteString requestData = ByteString.copyFrom(new byte[upSize]);
				Ping request = Ping.newBuilder().setProcessingTime(procTime).setPingData(requestData).setPongDataLength(downSize).build();
				Pong pong = myService.ping(controller, request);
				if ( pong.getPongData().size() != downSize ) {
					throw new Exception("Reply payload mismatch.");
				}
			}
			endTS = System.currentTimeMillis();
			log.info("BlockingCalls " + numCalls + " in " + (endTS-startTS)/1000 + "s");
			
		} finally {
			if ( channel != null ) {
				channel.close();
			}
			bootstrap.releaseExternalResources(); // check if this closes.
		}
	}
	
	
}
