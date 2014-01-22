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

import java.util.concurrent.ExecutorService;


/**
 * @author Peter Klauser
 *
 */
public interface RpcServerCallExecutor extends ExecutorService {

	/**
	 * Initiate Execution of the RPC call.
	 * 
	 * The executor guarantees to call's executorCallback always, once
	 * execution is finished or cancellation is finished. It may call
	 * onFinish only once and onCancel multiple times, since Thread
	 * race conditions make it hard to guarantee this.
	 * 
	 * The RpcServerCallExecutor will populate the call's Executor - before
	 * execution, so that on returning there will always be a reference to it
	 * to be able to cancel. 
	 * 
	 * Note execution could actually start and even FINISH before returning.
	 * 
	 * @param call
	 * @param executorCallback
	 */
	public void execute( PendingServerCallState call );
	
	/**
	 * Initiate Cancellation of the RPC call, which may be not started,
	 * in-process or already finishing or finished (but the calling thread
	 * didn't notice yet).
	 * 
	 * @param executor the executor registered in the callState during execute.
	 */
	public void cancel( Runnable executor );
	
}
