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
package com.googlecode.protobuf.pro.duplex;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.execute.ServerRpcController;
import com.googlecode.protobuf.pro.duplex.handler.RpcClientHandler;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogger;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.OobMessage;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.OobResponse;
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

	private static Logger log = LoggerFactory.getLogger(RpcClient.class);
	
	private AtomicInteger correlationId = new AtomicInteger(1);

	private final Map<Integer, PendingClientCallState> pendingRequestMap = new ConcurrentHashMap<Integer, PendingClientCallState>();
	
	private final PeerInfo clientInfo;
	private final PeerInfo serverInfo;
	private final boolean compression;
	private final ExtensionRegistry extensionRegistry;
	
	private Message onOobMessagePrototype;
	private RpcCallback<Message> onOobMessageFunction;
	
	private RpcServer rpcServer;
	private final RpcLogger rpcLogger;
	
	private final Channel channel;
	private final String channelName;
	
	public RpcClient( Channel channel, PeerInfo clientInfo, PeerInfo serverInfo, boolean compression, RpcLogger logger, ExtensionRegistry extensionRegistry ) {
		this.channel = channel;
		this.clientInfo = clientInfo;
		this.serverInfo = serverInfo;
		this.compression = compression;
		this.rpcLogger = logger;
		this.channelName = clientInfo.getName() + "->" + serverInfo.getName();
		this.extensionRegistry = extensionRegistry;
	}
	
	/**
	 * The channel name of the RpcClient.
	 * @return
	 */
	public String getChannelName() {
		return channelName;
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
		
		RpcRequest rpcRequest = RpcRequest.newBuilder()
			.setCorrelationId(correlationId)
			.setServiceIdentifier(state.getServiceIdentifier())
			.setMethodIdentifier(state.getMethodIdentifier())
			.setRequestBytes(request.toByteString())
			.setTimeoutMs(rpcController.getTimeoutMs())
			.build();
		WirePayload payload = WirePayload.newBuilder().setRpcRequest(rpcRequest).build();
		
		if ( log.isDebugEnabled() ) {
			log.debug("Sending ["+rpcRequest.getCorrelationId()+"]RpcRequest.");
		}
		
		if ( channel.isOpen() ) {
			registerPendingRequest(correlationId, state);

			channel.writeAndFlush(payload);
			
		} else {
			RpcError rpcError = RpcError.newBuilder().setCorrelationId(correlationId).setErrorMessage("Channel Closed").build();
			
			doLogRpc( state, rpcError, rpcError.getErrorMessage() );
			
			state.handleFailure(rpcError.getErrorMessage());
		}
	}

	/* (non-Javadoc)
	 * @see com.google.protobuf.BlockingRpcChannel#callBlockingMethod(com.google.protobuf.Descriptors.MethodDescriptor, com.google.protobuf.RpcController, com.google.protobuf.Message, com.google.protobuf.Message)
	 */
	@Override
	public Message callBlockingMethod(MethodDescriptor method,
			RpcController controller, Message request, Message responsePrototype)
			throws ServiceException {
		ClientRpcController rpcController = (ClientRpcController)controller;
		long deadlineTSNano = 0;
		if ( rpcController.getTimeoutMs() > 0 ) {
			deadlineTSNano = System.nanoTime() + rpcController.getTimeoutNanos();
		}
		
		BlockingRpcCallback callback = new BlockingRpcCallback();
		
		callMethod( method, rpcController, request, responsePrototype, callback );
        boolean interrupted = false;
		while(!callback.isDone()) {
			try {
				if ( deadlineTSNano > 0 ) {
					long timeToDeadlineNano = deadlineTSNano - System.nanoTime();
					if ( timeToDeadlineNano <= 0 ) {
						rpcController.getRpcClient().blockingCallTimeout(rpcController.getCorrelationId());
						// this will pre-emptively timeout this call and set the callback done flag before returning.
						// Issue25: infinite loop, assumption was race condition of timeout handling with correct response.
						// so we moved synchronization inside the loop, and make sure we exit.
						if ( !callback.isDone() ) {
							log.error("Issue25: not fixed - callback after timeout handling not finished. Please re-open issue.");
							// probably return null - since callback run() is not completed.
							break;
						}
					} else {
						int timeToDeadlineMs = (int)(timeToDeadlineNano / 1000000l);
						int remainderNano = (int)(timeToDeadlineNano - (timeToDeadlineMs*1000000l));
						// we wait at most until the deadline.
						synchronized(callback) {
							if ( !callback.isDone() ) {
								callback.wait(timeToDeadlineMs,remainderNano);
							}
						}
					}
				} else {
					// we wait indefinitely ( no timeout defined ).
					synchronized(callback) {
						if ( !callback.isDone() ) {
							callback.wait();
						}
					}
				}
			} catch (InterruptedException e) {
				if ( log.isDebugEnabled() ) {
					log.debug("Thread interrupted waiting in callBlockingMethod.", e);
				}
				interrupted = true;
				break; // Server side blocking call to client must exit here
			}
		}
        if ( interrupted ) { //Defect 8. Unsafe wait/notify loop.
			Thread.currentThread().interrupt();
        }
        if (rpcController.failed()) {
			throw new ServiceException(rpcController.errorText());
		}
        if (interrupted && !callback.isDone()) {
			throw new ServiceException("Blocking call interrupted.");
		}
		return callback.getMessage();
	}
	
	@Override
	public ClientRpcController newRpcController() {
		return new ClientRpcController(this);
	}
	
	@Override
	public String toString() {
		return getChannelName();
	}
	
	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#sendOobMessage(com.google.protobuf.Message)
	 */
	@Override
	public ChannelFuture sendOobMessage(Message message) {
		OobMessage msg = OobMessage.newBuilder()
				.setMessageBytes(message.toByteString())
				.build();
		WirePayload payload = WirePayload.newBuilder().setOobMessage(msg).build();
		
		if ( log.isDebugEnabled() ) {
			log.debug("Sending OobMessage.");
		}
		
		ChannelFuture future = channel.writeAndFlush(payload);

		doLogOobMessageOutbound(message);
		return future;
	}

	public void receiveOobMessage(OobMessage msg) {
		if ( onOobMessagePrototype != null && onOobMessageFunction != null ) {
			Message onMsg = null;
			try {
				onMsg = onOobMessagePrototype.newBuilderForType().mergeFrom(msg.getMessageBytes(),getExtensionRegistry()).build();

				onOobMessageFunction.run(onMsg);

				doLogOobMessageInbound(onMsg);
				
			} catch ( InvalidProtocolBufferException e ) {
				String errorMessage = "Invalid UncorrelatedMessage Protobuf.";
				
				//TODO doLog( state, rpcResponse, errorMessage );
				
				log.warn(errorMessage, e);
			}			
		} else {
			if ( log.isDebugEnabled() ) {
				log.debug("No onOobMessageFunction registered for received OobMessage.");
			}
		}
	}
	
	public void checkTimeouts( RpcTimeoutExecutor executor ) {
		List<Map.Entry<Integer,PendingClientCallState>> result = new ArrayList<Map.Entry<Integer,PendingClientCallState>>();
		result.addAll(pendingRequestMap.entrySet());
		
		for( Map.Entry<Integer,PendingClientCallState> call : result) {
			if ( call.getValue().isTimeoutExceeded() ) {
				RpcError rpcTimeout = RpcError.newBuilder().setCorrelationId(call.getKey()).setErrorMessage("Timeout").build();

				executor.timeout(this, rpcTimeout);
			}
		}
	}
	
	/**
	 * A blocking call thread calls this method in 
	 * {@link #callBlockingMethod(MethodDescriptor, RpcController, Message, Message)}
	 * 
	 * @param correlationid
	 */
	public void blockingCallTimeout( int correlationId ) {
		RpcError rpcTimeout = RpcError.newBuilder().setCorrelationId(correlationId).setErrorMessage("Timeout").build();
		error(rpcTimeout);
	}
	
	/**
	 * Receipt of an RpcError reply from a remote Peer.
	 * 
	 * @param rpcError
	 */
	public void error(RpcError rpcError) {
		if ( log.isDebugEnabled() ) {
			log.debug("Received ["+rpcError.getCorrelationId()+"]RpcError. ErrorMessage=" + rpcError.getErrorMessage());
		}
		PendingClientCallState state = removePendingRequest(rpcError.getCorrelationId());
		if ( state != null ) {
			
			doLogRpc( state, rpcError, rpcError.getErrorMessage() );
			
			state.handleFailure(rpcError.getErrorMessage());
		} else {
			// this can happen when we have cancellation and the server still responds.
			if ( log.isDebugEnabled() ) {
				log.debug("No PendingCallState found for correlationId " + rpcError.getCorrelationId());
			}
		}
	}
	
	/**
	 * The receipt of a RpcResponse from a remote Peer.
	 * 
	 * @param rpcResponse
	 */
	public void response(RpcResponse rpcResponse) {
		if ( log.isDebugEnabled() ) {
			log.debug("Received ["+rpcResponse.getCorrelationId()+"]RpcResponse.");
		}
		PendingClientCallState state = removePendingRequest(rpcResponse.getCorrelationId());
		if ( state != null ) {
			Message response = null;
			try {
				response = state.getResponsePrototype().newBuilderForType().mergeFrom(rpcResponse.getResponseBytes(),getExtensionRegistry()).build();

				doLogRpc( state, response, null );
				
				state.handleResponse( response );
			} catch ( InvalidProtocolBufferException e ) {
				String errorMessage = "Invalid Response Protobuf.";
				
				doLogRpc( state, rpcResponse, errorMessage );
				
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
	 * For use by {@link ServerRpcController} to send a server out-of-band message
	 * back to the client.
	 * 
	 * @param correlationId
	 * @param oobMessage
	 */
	public ChannelFuture sendOobResponse( String serviceName, int correlationId, Message oobMessage ) {
		OobResponse msg = OobResponse.newBuilder()
				.setCorrelationId(correlationId)
				.setMessageBytes(oobMessage.toByteString())
				.build();
		
		WirePayload payload = WirePayload.newBuilder().setOobResponse(msg).build();
		
		if ( log.isDebugEnabled() ) {
			log.debug("Sending ["+msg.getCorrelationId()+"]OobResponse.");
		}
		
		doLogOobResponseOutbound(serviceName, correlationId, oobMessage);
		
		return channel.writeAndFlush(payload);
	}
	
	/**
	 * For use by {@link RpcClientHandler} to dispatch an Out-of-Band server response
	 * message to client code.
	 * 
	 * @param serverMessage
	 */
	public void receiveOobResponse( OobResponse serverMessage ) {
		PendingClientCallState state = getPendingRequest(serverMessage.getCorrelationId());
		if ( state != null ) {
			Message processedMessage = state.getController().receiveOobResponse(serverMessage);
			if ( processedMessage != null ) {
				doLogOobResponseInbound(state, processedMessage);
			}
		} else {
			// this can happen when we have cancellation and the server still responds.
			if ( log.isDebugEnabled() ) {
				log.debug("No PendingClientCallState found for correlationId " + serverMessage.getCorrelationId());
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
			channel.writeAndFlush( payload ).awaitUninterruptibly();
			
			String errorMessage = "Cancel";
			
			doLogRpc(state, rpcCancel, errorMessage);

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
					
					doLogRpc( state, rpcError, rpcError.getErrorMessage() );
					
					state.handleFailure(rpcError.getErrorMessage());
				}
			}
		} while( pendingRequestMap.size() > 0 );
	}
	
	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#getPipeline()
	 */
	@Override
	public ChannelPipeline getPipeline() {
		return channel.pipeline();
	}
	
	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#setOobMessageCallback(com.google.protobuf.Message, com.google.protobuf.RpcCallback)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void setOobMessageCallback(Message responsePrototype,
			RpcCallback<? extends Message> onMessage) {
		onOobMessageFunction = (RpcCallback<Message>) onMessage;
		onOobMessagePrototype = responsePrototype;
	}

	/**
	 * Logger all information about a completed RPC call.
	 * @param state
	 * @param response
	 * @param errorMessage
	 */
	protected void doLogRpc( PendingClientCallState state, Message response, String errorMessage ) {
		if ( rpcLogger != null ) {
			rpcLogger.logCall(clientInfo, serverInfo, state.getMethodDesc().getFullName(), state.getRequest(), response, errorMessage, state.getController().getCorrelationId(), state.getStartTimestamp(), System.currentTimeMillis());
		}
	}
	
	/**
	 * Logger all information about the received OobResponse of an RPC call.
	 * @param state
	 * @param response
	 */
	protected void doLogOobResponseInbound( PendingClientCallState state, Message oobResponse) {
		if ( rpcLogger != null ) {
			rpcLogger.logOobResponse(clientInfo, serverInfo, oobResponse, state.getMethodDesc().getFullName(), state.getController().getCorrelationId(), System.currentTimeMillis());
		}
	}
	
	/**
	 * Logger all information about the received OobResponse of an RPC call.
	 * @param state
	 * @param response
	 */
	protected void doLogOobResponseOutbound( String serviceName, int correlationId, Message oobResponse) {
		if ( rpcLogger != null ) {
			rpcLogger.logOobResponse(serverInfo, clientInfo, oobResponse, serviceName, correlationId, System.currentTimeMillis());
		}
	}
	
	/**
	 * Logger all information about an outbound OobMessage.
	 */
	protected void doLogOobMessageOutbound( Message message ) {
		if ( rpcLogger != null ) {
			rpcLogger.logOobMessage(clientInfo, serverInfo, message, System.currentTimeMillis());
		}
	}
	
	/**
	 * Logger all information about an inbound OobMessage.
	 */
	protected void doLogOobMessageInbound( Message message ) {
		if ( rpcLogger != null ) {
			rpcLogger.logOobMessage(serverInfo, clientInfo, message, System.currentTimeMillis());
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

	private PendingClientCallState getPendingRequest(int seqId) {
		return pendingRequestMap.get(seqId);
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

		public boolean isTimeoutExceeded() {
			return controller.getTimeoutMs() > 0 && System.currentTimeMillis() > startTimestamp + controller.getTimeoutMs();
		}
		
		public String getServiceIdentifier() {
			return methodDesc.getService().getFullName(); //Issue 29: use full name
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
	 * @return whether this client's communication is compressed.
	 */
	public boolean isCompression() {
		return compression;
	}

	/**
	 * @return the rpcServer
	 */
	public RpcServer getRpcServer() {
		return rpcServer;
	}

	/**
	 * @param rpcServer the rpcServer to set
	 */
	public void setRpcServer(RpcServer rpcServer) {
		this.rpcServer = rpcServer;
	}

	/**
	 * @return the extensionRegistry
	 */
	public ExtensionRegistry getExtensionRegistry() {
		return extensionRegistry;
	}

}
