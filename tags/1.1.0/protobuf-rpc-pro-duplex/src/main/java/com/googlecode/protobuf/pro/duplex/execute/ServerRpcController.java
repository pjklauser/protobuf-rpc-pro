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
package com.googlecode.protobuf.pro.duplex.execute;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;

/**
 * @author Peter Klauser
 *
 */
public class ServerRpcController implements com.google.protobuf.RpcController {

	private String failureReason;
	private final AtomicBoolean canceled = new AtomicBoolean(false);
	private final AtomicBoolean cancelCallbackNotified = new AtomicBoolean(false);
	private RpcCallback<Object> cancelNotifyCallback;
	private RpcClient rpcClient;
	private int correlationId;
	
	public ServerRpcController( RpcClient rpcClient, int correlationId ) {
		this.rpcClient = rpcClient;
		this.correlationId = correlationId;
	}
	
	public static RpcClientChannel getRpcChannel( com.google.protobuf.RpcController controller ) {
		ServerRpcController c = (ServerRpcController)controller;
		return c.getRpcClient();
	}
	
	public String errorText() {
		throw new IllegalStateException("Client-side use only.");
	}

	public boolean failed() {
		throw new IllegalStateException("Client-side use only.");
	}

	public void startCancel() {
		this.canceled.set(true);
	}

	public boolean isCanceled() {
		return canceled.get();
	}

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

}