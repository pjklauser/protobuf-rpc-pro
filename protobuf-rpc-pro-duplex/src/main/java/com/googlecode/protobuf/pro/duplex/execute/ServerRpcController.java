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
package com.googlecode.protobuf.pro.duplex.execute;

import io.netty.channel.ChannelFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.LocalCallVariableHolder;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;

/**
 * @author Peter Klauser
 *
 */
public class ServerRpcController implements RpcController, LocalCallVariableHolder {

	private String failureReason;
	private final AtomicBoolean canceled = new AtomicBoolean(false);
	private final AtomicBoolean cancelCallbackNotified = new AtomicBoolean(false);
	private RpcCallback<Object> cancelNotifyCallback;
	private RpcClient rpcClient;
	private int correlationId;
	private String serviceName;
	
	/**
	 * A convenient store of call local variables.
	 * The values are not transmitted, just kept locally. Is cleared on {@link #reset()}
	 */
	private Map<String, Object> callLocalVariables;
	
	public ServerRpcController( RpcClient rpcClient, String serviceName, int correlationId ) {
		this.rpcClient = rpcClient;
		this.correlationId = correlationId;
		this.serviceName = serviceName;
	}
	
	public static ServerRpcController getRpcController( com.google.protobuf.RpcController controller ) {
		ServerRpcController c = (ServerRpcController)controller;
		return c;
	}

	public static RpcClientChannel getRpcChannel( com.google.protobuf.RpcController controller ) {
		ServerRpcController c = (ServerRpcController)controller;
		return c.getRpcClient();
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
		if ( callLocalVariables != null ) {
			callLocalVariables = new HashMap<String,Object>();
		}
		return callLocalVariables.put(key, value);
	}
	
	@Override
	public String errorText() {
		throw new IllegalStateException("Client-side use only.");
	}

	@Override
	public boolean failed() {
		throw new IllegalStateException("Client-side use only.");
	}

	@Override
	public void startCancel() {
		this.canceled.set(true);
	}

	@Override
	public boolean isCanceled() {
		return canceled.get();
	}

	@Override
	public void notifyOnCancel(RpcCallback<Object> callback) {
		if ( isCanceled() ) {
			callback.run(null);
		} else {
			cancelNotifyCallback = callback;
		}
	}

	@Override
	public void setFailed(String reason) {
		this.failureReason = reason;
	}

	@Override
	public void reset() {
		throw new IllegalStateException("Client-side use only.");
	}

	/**
	 * @return the failureReason
	 */
	public String getFailed() {
		return failureReason;
	}

	/**
	 * @return the cancelNotifyCallback
	 */
	public RpcCallback<Object> getCancelNotifyCallback() {
		return cancelNotifyCallback;
	}

	/**
	 * @return the correlationId
	 */
	public int getCorrelationId() {
		return correlationId;
	}

	/**
	 * @return the rpcClient
	 */
	public RpcClient getRpcClient() {
		return rpcClient;
	}

	/**
	 * @return the cancelCallbackNotified
	 */
	public boolean getAndSetCancelCallbackNotified() {
		return cancelCallbackNotified.getAndSet(true);
	}

	/**
	 * Send an out-of-band message related to this RPC call back to the client.
	 * 
	 * @param msg
	 */
	public ChannelFuture sendOobResponse( Message msg ) {
		return rpcClient.sendOobResponse(serviceName, correlationId, msg);
	}

	/**
	 * @return the serviceName
	 */
	public String getServiceName() {
		return serviceName;
	}
}