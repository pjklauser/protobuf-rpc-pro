/**
 *   Copyright 2010 Peter Klauser
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
package com.googlecode.protobuf.pro.duplex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogger;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.RpcCancel;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.RpcError;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.RpcRequest;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.RpcResponse;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.WirePayload;

/**
 * The RpcClient allows clients to send RpcRequests to an RpcServer
 * over the IO-Layer. The RpcClient will handle incoming RpcResponse
 * and RpcErrors coming back from the server, giving back to the
 * client callback.
 * 
 * The RpcClient allows the client to cancel an ongoing RpcRequest
 * before the RpcResponse or RpcError has been received. The RpcCancel
 * is sent over the IO-Layer to the server. The ongoing client call
 * is immediately failed.
 * 
 * @author Peter Klauser
 *
 */
public class RpcClient implements RpcClientChannel {

	private static Log log = LogFactory.getLog(RpcClient.class);
	
	private AtomicInteger correlationId = new AtomicInteger(1);

	private final Map<Integer, PendingClientCallState> pendingRequestMap = new ConcurrentHashMap<Integer, PendingClientCallState>();
	
	private final Channel channel;
	
	private final PeerInfo clientInfo;
	private final PeerInfo serverInfo;
	private final boolean compression;
	
	private RpcLogger rpcLogger;
	
	public RpcClient( Channel channel, PeerInfo clientInfo, PeerInfo serverInfo, boolean compression ) {
		this.channel = channel;
		this.clientInfo = clientInfo;
		this.serverInfo = serverInfo;
		this.compression = compression;
	}
	
	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#getPeerInfo()
	 */
	@Override
	public PeerInfo getPeerInfo() {
		return this.serverInfo;
	}
	

	/* (non-Javadoc)
	 * @see com.google.protobuf.RpcChannel#callMethod(com.google.protobuf.Descriptors.MethodDescriptor, com.google.protobuf.RpcController, com.google.protobuf.Message, com.google.protobuf.Message, com.google.protobuf.RpcCallback)
	 */
	@Override
	public void callMethod(MethodDescriptor method, RpcController controller,
			Message request, Message responsePrototype,
			RpcCallback<Message> done) {
		ClientRpcController rpcController = (ClientRpcController)controller;
		
		int correlationId = getNextCorrelationId();
		rpcController.setCorrelationId(correlationId);

		PendingClientCallState state = new PendingClientCallState(rpcController, method, responsePrototype, request, done);
		registerPendingRequest(correlationId, state);
		
		RpcRequest rpcRequest = RpcRequest.newBuilder()
			.setCorrelationId(correlationId)
			.setServiceIdentifier(state.getServiceIdentifier())
			.setMethodIdentifier(state.getMethodIdentifier())
			.setRequestBytes(request.toByteString())
			.build();
		WirePayload payload = WirePayload.newBuilder().setRpcRequest(rpcRequest).build();
		
		if ( log.isDebugEnabled() ) {
			log.debug("Sending ["+rpcRequest.getCorrelationId()+"]RpcRequest.");
		}
		
		channel.write(payload);
	}

	/* (non-Javadoc)
	 * @see com.google.protobuf.BlockingRpcChannel#callBlockingMethod(com.google.protobuf.Descriptors.MethodDescriptor, com.google.protobuf.RpcController, com.google.protobuf.Message, com.google.protobuf.Message)
	 */
	@Override
	public Message callBlockingMethod(MethodDescriptor method,
			RpcController controller, Message request, Message responsePrototype)
			throws ServiceException {
		BlockingRpcCallback callback = new BlockingRpcCallback();
		
		callMethod( method, controller, request, responsePrototype, callback );
        boolean interrupted = false;
        if ( !callback.isDone() ) {
			synchronized(callback) {
				while(!callback.isDone()) {
					try {
						callback.wait();
					} catch (InterruptedException e) {
						if ( log.isDebugEnabled() ) {
							log.debug("Thread interrupted waiting in callBlockingMethod.", e);
						}
						interrupted = true;
						break; // Server side blocking call to client must exit here
					}
				}
			}
		}
        if ( interrupted ) { //Defect 8. Unsafe wait/notify loop.
			Thread.currentThread().interrupt();
        }
        if (controller.failed()) {
			throw new ServiceException(controller.errorText());
		}
        if (interrupted && !callback.isDone()) {
			throw new ServiceException("Blocking call interrupted.");
		}
		return callback.getMessage();
	}
	
	@Override
	public RpcController newRpcController() {
		return new ClientRpcController(this);
	}
	
	@Override
	public String toString() {
		return "RpcClientChannel->" + getPeerInfo();
	}
	
	public void error(RpcError rpcError) {
		if ( log.isDebugEnabled() ) {
			log.debug("Received ["+rpcError.getCorrelationId()+"]RpcError. ErrorMessage=" + rpcError.getErrorMessage());
		}
		PendingClientCallState state = removePendingRequest(rpcError.getCorrelationId());
		if ( state != null ) {
			
			doLog( state, rpcError, rpcError.getErrorMessage() );
			
			state.handleFailure(rpcError.getErrorMessage());
		} else {
			// this can happen when we have cancellation and the server still responds.
			if ( log.isDebugEnabled() ) {
				log.debug("No PendingCallState found for correlationId " + rpcError.getCorrelationId());
			}
		}
	}
	
	public void response(RpcResponse rpcResponse) {
		if ( log.isDebugEnabled() ) {
			log.debug("Received ["+rpcResponse.getCorrelationId()+"]RpcResponse.");
		}
		PendingClientCallState state = removePendingRequest(rpcResponse.getCorrelationId());
		if ( state != null ) {
			Message response = null;
			try {
				response = state.getResponsePrototype().newBuilderForType().mergeFrom(rpcResponse.getResponseBytes()).build();

				doLog( state, response, null );
				
				state.handleResponse( response );
			} catch ( InvalidProtocolBufferException e ) {
				String errorMessage = "Invalid Response Protobuf.";
				
				doLog( state, rpcResponse, errorMessage );
				
				state.handleFailure(errorMessage);
			}
		} else {
			// this can happen when we have cancellation and the server still responds.
			if ( log.isDebugEnabled() ) {
				log.debug("No PendingClientCallState found for correlationId " + rpcResponse.getCorrelationId());
			}
		}
	}
	
	/**
	 * Initiate cancellation of RPC call. 
	 * 
	 * We do not expect a server response once cancellation is started.
	 * The done.callback(null) is performed immediately. The controller's
	 * failed() text indicates "Cancel" as failure reason.
	 * 
	 * @param correlationId
	 */
	void startCancel(int correlationId ) {
		PendingClientCallState state = removePendingRequest(correlationId);
		if ( state != null ) {
			RpcCancel rpcCancel = RpcCancel.newBuilder().setCorrelationId(correlationId).build();
			if ( log.isDebugEnabled() ) {
				log.debug("Sending ["+rpcCancel.getCorrelationId()+"]RpcCancel.");
			}
			WirePayload payload = WirePayload.newBuilder().setRpcCancel(rpcCancel).build();
			channel.write( payload ).awaitUninterruptibly();
			
			String errorMessage = "Cancel";
			
			doLog(state, rpcCancel, errorMessage);

			state.handleFailure(errorMessage);
			
		} else {
			// this can happen if the server call completed in the meantime.
			if ( log.isDebugEnabled() ) {
				log.debug("No PendingClientCallState found for correlationId " + correlationId);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#close()
	 */
	@Override
	public void close() {
		channel.close().awaitUninterruptibly();
	}
	
	public void handleClosure() {
		do {
			//Defect Nr.8 Race condition with new client request being received on closure.
			List<Integer> pendingCallIds = new ArrayList<Integer>();
			pendingCallIds.addAll(pendingRequestMap.keySet());
			for( Integer correlationId : pendingCallIds ) {
				PendingClientCallState state = removePendingRequest(correlationId);
				if ( state != null ) {
					RpcError rpcError = RpcError.newBuilder().setCorrelationId(correlationId).setErrorMessage("Forced Closure").build();
					
					doLog( state, rpcError, rpcError.getErrorMessage() );
					
					state.handleFailure(rpcError.getErrorMessage());
				}
			}
		} while( pendingRequestMap.size() > 0 );
	}
	
	protected void doLog( PendingClientCallState state, Message response, String errorMessage ) {
		if ( rpcLogger != null ) {
			rpcLogger.logCall(clientInfo, serverInfo, state.getMethodDesc().getFullName(), state.getRequest(), response, errorMessage, state.getController().getCorrelationId(), state.getStartTimestamp(), System.currentTimeMillis());
		}
	}
	
	private int getNextCorrelationId() {
		return correlationId.getAndIncrement();
	}
	
	private void registerPendingRequest(int seqId, PendingClientCallState state) {
		if (pendingRequestMap.containsKey(seqId)) {
			throw new IllegalArgumentException("State already registered");
		}
		pendingRequestMap.put(seqId, state);
	}

	private PendingClientCallState removePendingRequest(int seqId) {
		return pendingRequestMap.remove(seqId);
	}

	public static class ClientRpcController implements RpcController {

		private RpcClient rpcClient;
		private int correlationId;
		
		private String reason;
		private boolean failed;
		
		public ClientRpcController( RpcClient rpcClient ) {
			this.rpcClient = rpcClient;
		}
		
		public String errorText() {
			return reason;
		}

		public boolean failed() {
			return failed;
		}

		public boolean isCanceled() {
			throw new IllegalStateException("Serverside use only.");
		}

		public void notifyOnCancel(RpcCallback<Object> callback) {
			throw new IllegalStateException("Serverside use only.");
		}

		public void reset() {
			reason = null;
			failed = false;
			correlationId = 0;
		}

		public void setFailed(String reason) {
			this.reason = reason;
			this.failed = true;
		}

		public void startCancel() {
			rpcClient.startCancel(correlationId);
		}

		public int getCorrelationId() {
			return this.correlationId;
		}
		
		public void setCorrelationId( int correlationId ) {
			this.correlationId = correlationId;
		}

	}

	private static class BlockingRpcCallback implements RpcCallback<Message> {

		private boolean done = false;
		private Message message;
		
		public void run(Message message) {
			this.message = message;
			synchronized(this) {
				done = true;
				notify();
			}
		}
		
		public Message getMessage() {
			return message;
		}
		
		public boolean isDone() {
			return done;
		}
	}
		

	private static class PendingClientCallState {
		
		private final ClientRpcController controller;
		private final RpcCallback<Message> callback; 
		private final MethodDescriptor methodDesc;
		private final Message responsePrototype;
		private final long startTimestamp;
		private final Message request;
		
		public PendingClientCallState(ClientRpcController controller, MethodDescriptor methodDesc, Message responsePrototype, Message request, RpcCallback<Message> callback) {
			this.controller = controller;
			this.methodDesc = methodDesc;
			this.responsePrototype = responsePrototype;
			this.callback = callback;
			this.startTimestamp = System.currentTimeMillis();
			this.request = request;
		}

		public String getServiceIdentifier() {
			return methodDesc.getService().getName();
		}
		
		public String getMethodIdentifier() {
			return methodDesc.getName();
		}
		
		public void handleResponse( Message response ) {
			callback(response);
		}
		
		public void handleFailure( String message ) {
			controller.setFailed(message);
			callback(null);
		}

		private void callback(Message message) {
			if ( callback != null ) {
				callback.run(message);
			}
		}

		/**
		 * @return the startTimestamp
		 */
		public long getStartTimestamp() {
			return startTimestamp;
		}

		/**
		 * @return the request
		 */
		public Message getRequest() {
			return request;
		}

		/**
		 * @return the controller
		 */
		public ClientRpcController getController() {
			return controller;
		}

		/**
		 * @return the methodDesc
		 */
		public MethodDescriptor getMethodDesc() {
			return methodDesc;
		}

		/**
		 * @return the responsePrototype
		 */
		public Message getResponsePrototype() {
			return responsePrototype;
		}
	}

	/**
	 * @return the clientInfo
	 */
	public PeerInfo getClientInfo() {
		return clientInfo;
	}

	/**
	 * @return the serverInfo
	 */
	public PeerInfo getServerInfo() {
		return serverInfo;
	}

	/**
	 * @return the channel
	 */
	public Channel getChannel() {
		return channel;
	}

	/**
	 * @return the callLogger
	 */
	public RpcLogger getCallLogger() {
		return rpcLogger;
	}

	/**
	 * @param callLogger the callLogger to set
	 */
	public void setCallLogger(RpcLogger callLogger) {
		this.rpcLogger = callLogger;
	}

	/**
	 * @return whether this client's communication is compressed.
	 */
	public boolean isCompression() {
		return compression;
	}

}
