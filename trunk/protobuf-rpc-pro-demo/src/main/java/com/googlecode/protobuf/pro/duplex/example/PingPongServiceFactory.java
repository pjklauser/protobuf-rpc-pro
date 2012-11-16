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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.RpcClient.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService.BlockingInterface;
import com.googlecode.protobuf.pro.duplex.execute.ServerRpcController;

/**
 * PingService
 * 
 * @author Peter Klauser
 *
 */
public class PingPongServiceFactory {
	
	private static void doProcessing( RpcController controller, int processingTime ) {
		long processUntil = System.currentTimeMillis() + processingTime;
		do {
				long sleepDuration = processUntil - System.currentTimeMillis();
				if ( sleepDuration > 0 ) {
					try {
						Thread.sleep(sleepDuration);
					} catch ( InterruptedException e ) {
						// RpcCancel interrupts Thread.sleep and NIO waits.
						
						Thread.currentThread().interrupt(); // propagate just in case we have later IO operations
						// this will be cleared after the RPC call returns.
						if ( controller.isCanceled() ) {
							// just get out
							return;
						}
					}
				}
			
		} while( System.currentTimeMillis() < processUntil );
	}
	
	public static class BlockingPingService implements PingService.BlockingInterface {
		@Override
		public Pong ping(RpcController controller, Ping request)
				throws ServiceException {
			doProcessing(controller, request.getNumber());
			
			Pong response = Pong.newBuilder().setNumber(request.getNumber()).setPongData(request.getPingData()).build();
			return response;
		}
	}
	
	public static class BlockingPongService implements PongService.BlockingInterface {
		@Override
		public Ping pong(RpcController controller, Pong request)
				throws ServiceException {
			doProcessing(controller, request.getNumber());
			
			Ping response = Ping.newBuilder().setNumber(request.getNumber()).setPingData(request.getPongData()).build();
			return response;
		}

	}
	
	public static class NonBlockingPongServer implements PongService.Interface {

		@Override
		public void pong(RpcController controller, Pong request,
				RpcCallback<Ping> done) {
			doProcessing(controller, request.getNumber());
			if ( controller.isCanceled() ) {
				done.run(null);
				return;
			}
			Ping response = Ping.newBuilder().setNumber(request.getNumber()).setPingData(request.getPongData()).build();
			done.run(response);
		}
		
	}
	
	public static class NonBlockingPingServer implements PingService.Interface {

		@Override
		public void ping(RpcController controller, Ping request,
				RpcCallback<Pong> done) {
			doProcessing(controller, request.getNumber());
			if ( controller.isCanceled() ) {
				done.run(null);
				return;
			}
			Pong response = Pong.newBuilder().setNumber(request.getNumber()).setPongData(request.getPingData()).build();
			done.run(response);
		}
	}
	
    /**
     * This NonBlockingPongingPingServiceImpl services a ping() call.
     * 
     * The ping() call will reverse call pong() of the client who's calling.
     *
     * The blocking pong() call contains the same data as the ping
     * call. The final response of the server will be what
     * the client replied in the pong response.
     */
	static class NonBlockingPongingPingServer implements PingService.Interface {

		private int pongTimeoutMs;
		private int pongDurationMs;
		
		public NonBlockingPongingPingServer(int pongDurationMs, int pongTimeoutMs) {
			this.pongTimeoutMs = pongTimeoutMs;
			this.pongDurationMs = pongDurationMs;
		}


		@Override
		public void ping(RpcController controller, Ping request, RpcCallback<Pong> done) {
			doProcessing(controller, request.getNumber());
			if ( controller.isCanceled() ) {
				done.run(null);
				return;
			}
			
			// call the pong of the client calling ping.
			RpcClientChannel channel = ServerRpcController.getRpcChannel(controller);
			BlockingInterface clientService = PongService.newBlockingStub(channel);
			ClientRpcController clientController = channel.newRpcController();
			clientController.setTimeoutMs(pongTimeoutMs);
			
			Ping clientResponse = null;
			try {
				Pong clientRequest = Pong.newBuilder().setNumber(pongDurationMs).setPongData(request.getPingData()).build();

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
	/**
     * This PingService services a ping() call in a blocking way.
     * 
     * The ping() call will reverse call pong() of
     * the client who's calling.
     *
     * The blocking pong() call contains the same data as the ping
     * call. The final response of the server will be what
     * the client replied in the pong response.
     */
	public static class BlockingPongingPingServer implements PingService.BlockingInterface {

		private int pongTimeoutMs;
		private int pongDurationMs;
		
		public BlockingPongingPingServer(int pongDurationMs, int pongTimeoutMs) {
			this.pongTimeoutMs = pongTimeoutMs;
			this.pongDurationMs = pongDurationMs;
		}
		
		@Override
		public Pong ping(RpcController controller, Ping request) throws ServiceException {
			doProcessing(controller, request.getNumber());
			if ( controller.isCanceled() ) {
				return null;
			}
			
			RpcClientChannel channel = ServerRpcController.getRpcChannel(controller);
			BlockingInterface clientService = PongService.newBlockingStub(channel);
			ClientRpcController clientController = channel.newRpcController();
			clientController.setTimeoutMs(pongTimeoutMs);
			
			Pong clientRequest = Pong.newBuilder().setNumber(pongDurationMs).setPongData(request.getPingData()).build();

			Ping clientResponse = clientService.pong(clientController, clientRequest);
			Pong response = Pong.newBuilder().setNumber(clientResponse.getNumber()).setPongData(clientResponse.getPingData()).build();
			return response;
		}
	}
}
