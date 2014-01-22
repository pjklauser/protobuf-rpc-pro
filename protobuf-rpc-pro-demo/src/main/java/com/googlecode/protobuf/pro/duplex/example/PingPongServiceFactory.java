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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPongService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.ExtendedPing;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.ExtendedPong;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPongService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.PercentComplete;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.PercentComplete.OperationName;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Pong;
import com.googlecode.protobuf.pro.duplex.execute.ServerRpcController;

/**
 * PingService
 * 
 * @author Peter Klauser
 *
 */
public class PingPongServiceFactory {
	
	private static Logger log = LoggerFactory.getLogger(PingPongServiceFactory.class);

	private static void doPercentCompleteProcessing( RpcController controller, int processingTime, OperationName operationName, int seqNo ) {
		ServerRpcController rpcController = ServerRpcController.getRpcController(controller);
		
		long processStart = System.currentTimeMillis();
		long processUntil = System.currentTimeMillis() + processingTime;
		do {
				long sleepDuration = Math.min(1000, processUntil - System.currentTimeMillis());
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

				long total = processUntil - processStart;
				long processed = System.currentTimeMillis() - processStart;
				float percentComplete = ( processed*100.0f/total);
				
				PercentComplete pc = PercentComplete.newBuilder().setOp(operationName).setSequenceNo(seqNo).setPercentageComplete(percentComplete).build();
				rpcController.sendOobResponse(pc);
				
		} while( System.currentTimeMillis() < processUntil );

		PercentComplete pc = PercentComplete.newBuilder().setOp(operationName).setSequenceNo(seqNo).setPercentageComplete(100.0f).build();
		rpcController.sendOobResponse(pc);
	}
	
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
	
	public static class BlockingPingServer implements BlockingPingService.BlockingInterface {
		@Override
		public Pong ping(RpcController controller, Ping request)
				throws ServiceException {
			if ( request.getPingPercentComplete() ) {
				doPercentCompleteProcessing(controller,request.getPingDurationMs(),OperationName.PING, request.getSequenceNo());
			} else {
				doProcessing(controller, request.getPingDurationMs());
			}
			
			Pong response = Pong.newBuilder().setSequenceNo(request.getSequenceNo()).setPongData(request.getPingPayload()).build();
			return response;
		}
	}
	
	public static class BlockingPongServer implements BlockingPongService.BlockingInterface {
		@Override
		public Ping pong(RpcController controller, Pong request)
				throws ServiceException {
			if ( request.getPongPercentComplete() ) {
				doPercentCompleteProcessing(controller,request.getPongDurationMs(),OperationName.PONG, request.getSequenceNo());
			} else {
				doProcessing(controller, request.getPongDurationMs());
			}
			
			Ping response = Ping.newBuilder().setSequenceNo(request.getSequenceNo()).setPingPayload(request.getPongData()).build();
			return response;
		}

	}
	
	public static class NonBlockingPongServer implements NonBlockingPongService.Interface {

		@Override
		public void pong(RpcController controller, Pong request,
				RpcCallback<Ping> done) {
			if ( request.getPongPercentComplete() ) {
				doPercentCompleteProcessing(controller,request.getPongDurationMs(),OperationName.PONG, request.getSequenceNo());
			} else {
				doProcessing(controller, request.getPongDurationMs());
			}
			if ( controller.isCanceled() ) {
				done.run(null);
				return;
			}
			Ping response = Ping.newBuilder().setSequenceNo(request.getSequenceNo()).setPingPayload(request.getPongData()).build();
			done.run(response);
		}
		
	}
	
	public static class NonBlockingPingServer implements NonBlockingPingService.Interface {

		@Override
		public void ping(RpcController controller, Ping request,
				RpcCallback<Pong> done) {
			if ( request.getPingPercentComplete() ) {
				doPercentCompleteProcessing(controller,request.getPingDurationMs(),OperationName.PING, request.getSequenceNo());
			} else {
				doProcessing(controller, request.getPingDurationMs());
			}
			if ( controller.isCanceled() ) {
				done.run(null);
				return;
			}
			Pong response = Pong.newBuilder().setSequenceNo(request.getSequenceNo()).setPongData(request.getPingPayload()).build();
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
	public static class NonBlockingPongingPingServer implements NonBlockingPingService.Interface {

		@Override
		public void ping(RpcController controller, Ping request, RpcCallback<Pong> done) {
			if ( request.getPingPercentComplete() ) {
				doPercentCompleteProcessing(controller,request.getPingDurationMs(),OperationName.PING, request.getSequenceNo());
			} else {
				doProcessing(controller, request.getPingDurationMs());
			}
			if ( controller.isCanceled() ) {
				done.run(null);
				return;
			}
			
			Integer ext = request.getExtension(ExtendedPing.extendedIntField);
			
			// must call the blocking or non blocking pong of the client calling ping.
			if ( request.getPongRequired() ) {
				RpcClientChannel channel = ServerRpcController.getRpcChannel(controller);
				
				if ( request.getPongBlocking() ) {
					BlockingPongService.BlockingInterface clientService = BlockingPongService.newBlockingStub(channel);
					ClientRpcController clientController = channel.newRpcController();
					clientController.setTimeoutMs(request.getPongTimeoutMs());
					
					Ping clientResponse = null;
					try {
						Pong.Builder clientRequestBuilder = Pong.newBuilder().setSequenceNo(request.getSequenceNo()).setPongDurationMs(request.getPongDurationMs()).setPongData(request.getPingPayload());
						if ( ext != null ) {
							clientRequestBuilder.setExtension(ExtendedPong.extendedIntField, ext);
						}
						Pong clientRequest = clientRequestBuilder.build();
						
						clientResponse = clientService.pong(clientController, clientRequest);
						if ( ext != null ) {
							Integer repliedExt=clientResponse.getExtension(ExtendedPing.extendedIntField);
							if ( repliedExt == null ) {
								log.warn("Did not receive extension in reply to pong.");
							} else if ( repliedExt.intValue() != ext.intValue() ) {
								log.warn("Extension value mismatch in reply to pong.");
							}
						}
					} catch ( ServiceException e ) {
						controller.setFailed(e.getMessage());
						done.run(null);
						return;
					}
					Pong.Builder responseBuilder = Pong.newBuilder().setSequenceNo(clientResponse.getSequenceNo()).setPongData(clientResponse.getPingPayload());
					if ( ext != null ) {
						responseBuilder.setExtension(ExtendedPong.extendedIntField, ext);
					}
					Pong response = responseBuilder.build();
					done.run(response);
				} else {
					NonBlockingPongService.BlockingInterface clientService = NonBlockingPongService.newBlockingStub(channel);
					ClientRpcController clientController = channel.newRpcController();
					clientController.setTimeoutMs(request.getPongTimeoutMs());
					
					Ping clientResponse = null;
					try {
						Pong.Builder clientRequestBuilder = Pong.newBuilder().setSequenceNo(request.getSequenceNo()).setPongDurationMs(request.getPongDurationMs()).setPongData(request.getPingPayload());
						if ( ext != null ) {
							clientRequestBuilder.setExtension(ExtendedPong.extendedIntField, ext);
						}
						Pong clientRequest = clientRequestBuilder.build();

						clientResponse = clientService.pong(clientController, clientRequest);
						if ( ext != null ) {
							Integer repliedExt=clientResponse.getExtension(ExtendedPing.extendedIntField);
							if ( repliedExt == null ) {
								log.warn("Did not receive extension in reply to pong.");
							} else if ( repliedExt.intValue() != ext.intValue() ) {
								log.warn("Extension value mismatch in reply to pong.");
							}
						}
					} catch ( ServiceException e ) {
						controller.setFailed(e.getMessage());
						done.run(null);
						return;
					}
					Pong.Builder responseBuilder = Pong.newBuilder().setSequenceNo(clientResponse.getSequenceNo()).setPongData(clientResponse.getPingPayload());
					if ( ext != null ) {
						responseBuilder.setExtension(ExtendedPong.extendedIntField, ext);
					}
					Pong response = responseBuilder.build();
					done.run(response);
				}
				
			} else {
				Pong.Builder responseBuilder = Pong.newBuilder().setSequenceNo(request.getSequenceNo()).setPongData(request.getPingPayload());
				if ( ext != null ) {
					responseBuilder.setExtension(ExtendedPong.extendedIntField, ext);
				}
				Pong response = responseBuilder.build();
				done.run(response);
			}
		}
	}
	
	/**
     * This PingService services a ping() call in a blocking way.
     * 
     * The ping() call will optionally reverse call pong() of
     * the client who's calling.
     *
     * The pong() call contains the same data as the ping
     * call. The final response of the server will be what
     * the client replied in the pong response.
     */
	public static class BlockingPongingPingServer implements BlockingPingService.BlockingInterface {

		@Override
		public Pong ping(RpcController controller, Ping request) throws ServiceException {
			if ( request.getPingPercentComplete() ) {
				doPercentCompleteProcessing(controller,request.getPingDurationMs(),OperationName.PING, request.getSequenceNo());
			} else {
				doProcessing(controller, request.getPingDurationMs());
			}
			if ( controller.isCanceled() ) {
				return null;
			}
			
			Integer ext = request.getExtension(ExtendedPing.extendedIntField);
			
			Pong response = null;
			if ( request.getPongRequired() ) {
				RpcClientChannel channel = ServerRpcController.getRpcChannel(controller);
				if ( request.getPongBlocking() ) {
					BlockingPongService.BlockingInterface clientService = BlockingPongService.newBlockingStub(channel);
					ClientRpcController clientController = channel.newRpcController();
					clientController.setTimeoutMs(request.getPongTimeoutMs());
					
					Pong.Builder clientRequestBuilder = Pong.newBuilder().setSequenceNo(request.getSequenceNo()).setPongDurationMs(request.getPongDurationMs()).setPongData(request.getPingPayload());
					if ( ext != null ) {
						clientRequestBuilder.setExtension(ExtendedPong.extendedIntField, ext);
					}
					Pong clientRequest = clientRequestBuilder.build();
		
					Ping clientResponse = clientService.pong(clientController, clientRequest);
					if ( ext != null ) {
						Integer repliedExt=clientResponse.getExtension(ExtendedPing.extendedIntField);
						if ( repliedExt == null ) {
							log.warn("Did not receive extension in reply to pong.");
						} else if ( repliedExt.intValue() != ext.intValue() ) {
							log.warn("Extension value mismatch in reply to pong.");
						}
					}
					
					Pong.Builder responseBuilder = Pong.newBuilder().setSequenceNo(clientResponse.getSequenceNo()).setPongData(clientResponse.getPingPayload());
					if ( ext != null ) {
						responseBuilder.setExtension(ExtendedPong.extendedIntField, ext);
					}
					response = responseBuilder.build();
				} else {
					NonBlockingPongService.BlockingInterface clientService = NonBlockingPongService.newBlockingStub(channel);
					ClientRpcController clientController = channel.newRpcController();
					clientController.setTimeoutMs(request.getPongTimeoutMs());
					
					Pong.Builder clientRequestBuilder = Pong.newBuilder().setSequenceNo(request.getSequenceNo()).setPongDurationMs(request.getPongDurationMs()).setPongData(request.getPingPayload());
					if ( ext != null ) {
						clientRequestBuilder.setExtension(ExtendedPong.extendedIntField, ext);
					}
					Pong clientRequest = clientRequestBuilder.build();
		
					Ping clientResponse = clientService.pong(clientController, clientRequest);
					if ( ext != null ) {
						Integer repliedExt=clientResponse.getExtension(ExtendedPing.extendedIntField);
						if ( repliedExt == null ) {
							log.warn("Did not receive extension in reply to pong.");
						} else if ( repliedExt.intValue() != ext.intValue() ) {
							log.warn("Extension value mismatch in reply to pong.");
						}
					}

					Pong.Builder responseBuilder = Pong.newBuilder().setSequenceNo(clientResponse.getSequenceNo()).setPongData(clientResponse.getPingPayload());
					if ( ext != null ) {
						responseBuilder.setExtension(ExtendedPong.extendedIntField, ext);
					}
					response = responseBuilder.build();
				}
			} else {
				Pong.Builder responseBuilder = Pong.newBuilder().setSequenceNo(request.getSequenceNo()).setPongData(request.getPingPayload());
				if ( ext != null ) {
					responseBuilder.setExtension(ExtendedPong.extendedIntField, ext);
				}
				response = responseBuilder.build();
			}
			return response;
		}
	}
}
