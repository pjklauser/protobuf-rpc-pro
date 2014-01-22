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

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;

/**
 * @author Peter Klauser
 *
 */
public class PendingServerCallState {
	
	private final RpcServerExecutorCallback executorCallback;
	private final Service service;
	private final BlockingService blockingService;
	private final ServerRpcController controller;
	private final MethodDescriptor methodDesc;
	private final Message request;
	private final long startTS;
	private final int timeoutMs;
	
	/**
	 * The executor is set by the RpcServerCallExecutor before starting the call,
	 * and removed by the RpcServerCallExecutor after call completion, but before 
	 * calling the RpcServerExecutionCallback.
	 * 
	 * The RpcServer will remove the executor before initiating a cancel.
	 */
	private Runnable executor;
	
	public PendingServerCallState(RpcServerExecutorCallback executorCallback, Service service, ServerRpcController controller, MethodDescriptor methodDesc, Message request, long startTS, int timeoutMs) {
		this.executorCallback = executorCallback;
		this.service = service;
		this.blockingService = null;
		this.controller = controller;
		this.methodDesc = methodDesc;
		this.request = request;
		this.startTS = startTS;
		this.timeoutMs = timeoutMs;
	}

	public PendingServerCallState(RpcServerExecutorCallback executorCallback, BlockingService service, ServerRpcController controller, MethodDescriptor methodDesc, Message request, long startTS, int timeoutMs) {
		this.executorCallback = executorCallback;
		this.service = null;
		this.blockingService = service;
		this.controller = controller;
		this.methodDesc = methodDesc;
		this.request = request;
		this.startTS = startTS;
		this.timeoutMs = timeoutMs;
	}

	/**
	 * @return the controller
	 */
	public ServerRpcController getController() {
		return controller;
	}

	public boolean isTimeoutExceeded() {
		return timeoutMs > 0 && System.currentTimeMillis() > startTS + timeoutMs;
	}
	
	/**
	 * @return the request
	 */
	public Message getRequest() {
		return request;
	}

	/**
	 * @return the startTS
	 */
	public long getStartTS() {
		return startTS;
	}

	/**
	 * @return the methodDesc
	 */
	public MethodDescriptor getMethodDesc() {
		return methodDesc;
	}

	/**
	 * @return the executor
	 */
	public Runnable getExecutor() {
		return executor;
	}

	/**
	 * @param executor the executor to set
	 */
	public void setExecutor(Runnable executor) {
		this.executor = executor;
	}

	/**
	 * @return the service
	 */
	public Service getService() {
		return service;
	}

	/**
	 * @return the blockingService
	 */
	public BlockingService getBlockingService() {
		return blockingService;
	}

	/**
	 * @return the executorCallback
	 */
	public RpcServerExecutorCallback getExecutorCallback() {
		return executorCallback;
	}

	/**
	 * @return the timeoutMs
	 */
	public int getTimeoutMs() {
		return timeoutMs;
	}


}
