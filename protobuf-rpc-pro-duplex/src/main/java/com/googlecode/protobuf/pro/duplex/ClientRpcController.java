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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.OobResponse;

public class ClientRpcController implements RpcController, LocalCallVariableHolder {

	private static Logger log = LoggerFactory.getLogger(ClientRpcController.class);

	private RpcClient rpcClient;
	private int correlationId;
	
	private String reason;
	private boolean failed;
	
	/**
	 * Timeout in milliseconds, after which the RPC call times out and
	 * no longer will expect a response.
	 */
	private int timeoutMs = 0;
	
	/**
	 * Register an asynchronous callback function for ServerMessages
	 * sent from the Server back to the initiating Client during
	 * an RPC call processing on the server.
	 * 
	 * Note: resetting the RPC controller will remove any registered
	 * callback function.
	 */
	private RpcCallback<Message> onOobResponseFunction;
	
	/**
	 * The type of Protobuf message for the ServerMessages.
	 */
	private Message onOobResponsePrototype;
	
	/**
	 * A convenient store of call local variables.
	 * The values are not transmitted, just kept locally. Is cleared on {@link #reset()}
	 */
	private Map<String, Object> callLocalVariables;
	
	public ClientRpcController( RpcClient rpcClient ) {
		this.rpcClient = rpcClient;
	}
	
	@Override
	public synchronized Object getCallLocalVariable( String key ) {
		if ( callLocalVariables != null ) {
			return callLocalVariables.get(key);
		}
		return null;
	}

	@Override
	public synchronized Object storeCallLocalVariable( String key, Object value ) {
		if ( callLocalVariables == null ) {
			callLocalVariables = new HashMap<String,Object>();
		}
		return callLocalVariables.put(key, value);
	}
	
	@Override
	public String errorText() {
		return reason;
	}

	@Override
	public boolean failed() {
		return failed;
	}

	@Override
	public boolean isCanceled() {
		throw new IllegalStateException("Serverside use only.");
	}

	@Override
	public void notifyOnCancel(RpcCallback<Object> callback) {
		throw new IllegalStateException("Serverside use only.");
	}

	@Override
	public void reset() {
		reason = null;
		failed = false;
		correlationId = 0;
		onOobResponseFunction = null;
		onOobResponsePrototype = null;
		timeoutMs = 0;
		if ( callLocalVariables != null ) {
			callLocalVariables.clear();
		}
	}

	@Override
	public void setFailed(String reason) {
		this.reason = reason;
		this.failed = true;
	}

	@Override
	public void startCancel() {
		rpcClient.startCancel(correlationId);
	}

	public int getCorrelationId() {
		return this.correlationId;
	}
	
	public void setCorrelationId( int correlationId ) {
		this.correlationId = correlationId;
	}

	@SuppressWarnings("unchecked")
	public void setOobResponseCallback(Message responsePrototype,
			RpcCallback<? extends Message> onMessage) {
		onOobResponseFunction = (RpcCallback<Message>) onMessage;
		onOobResponsePrototype = responsePrototype;
	}
	
	/**
	 * 
	 * @param msg
	 * @return true the message processed, null if none processed.
	 */
	public Message receiveOobResponse(OobResponse msg) {
		if ( msg.getCorrelationId() != correlationId ) {
			// only possible with race condition on client reset and re-use when a server message
			// comes back
			log.info("Correlation mismatch client " + correlationId + " OobResponse " + msg.getCorrelationId());
			
			return null; // don't process the server message anymore in this case.
		}
		if ( onOobResponsePrototype != null && onOobResponseFunction != null ) {
			Message onMsg = null;
			try {
				onMsg = onOobResponsePrototype.newBuilderForType().mergeFrom(msg.getMessageBytes()).build();

				onOobResponseFunction.run(onMsg);
				
				return onMsg;
			} catch ( InvalidProtocolBufferException e ) {
				String errorMessage = "Invalid OobResponse Protobuf for correlationId " + correlationId;
				
				log.warn(errorMessage, e);
			}			
			
		} else {
			if ( log.isDebugEnabled() ) {
				log.debug("No onOobResponseCallbackFunction registered for correlationId " + correlationId);
			}
		}
		return null;
	}

	/**
	 * @return the timeoutMs
	 */
	public int getTimeoutMs() {
		return timeoutMs;
	}

	/**
	 * @return the timeoutNanos
	 */
	public long getTimeoutNanos() {
		return timeoutMs*1000000l;
	}

	/**
	 * @param timeoutMs the timeoutMs to set
	 */
	public void setTimeoutMs(int timeoutMs) {
		this.timeoutMs = timeoutMs;
	}

	/**
	 * @return the rpcClient
	 */
	public RpcClient getRpcClient() {
		return rpcClient;
	}
}
