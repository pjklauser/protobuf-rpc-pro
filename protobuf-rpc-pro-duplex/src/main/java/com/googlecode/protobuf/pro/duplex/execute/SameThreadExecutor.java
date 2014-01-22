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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;

/**
 * An RpcServerCallExecutor which directly calls the RPC service using the same
 * thread that is processing the IO data. This strategy is most efficient however
 * succeptable to deadlock if you are intending to perform reverse RPC calls from
 * server to SAME client when servicing an RPC call. In this case use 
 * {@link ThreadPoolCallExecutor}.
 * 
 * Furthermore RPC call cancellation works differently with this execution strategy.
 * In Netty, the same IO-Thread handles message receipt for a single TCP socket
 * connection, therefore although this code allows for cancellation of the RPC
 * call, in practice the cancellation will always arrive after the call has finished
 * processing ( because in this model the same Thread handles the cancel and the RPC 
 * call ).
 * 
 * @author Peter Klauser
 *
 */
public class SameThreadExecutor implements RpcServerCallExecutor {

	private static Logger log = LoggerFactory.getLogger(SameThreadExecutor.class);
	
	//weak hashmap of threads
	WeakHashMap<Thread, PendingServerCallState> runningCalls = new WeakHashMap<Thread,PendingServerCallState>();
	
	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor#execute(com.googlecode.protobuf.pro.duplex.execute.PendingServerCallState)
	 */
	@Override
	public void execute(PendingServerCallState call) {

		runningCalls.put(Thread.currentThread(), call);
		call.setExecutor((Runnable)Thread.currentThread());
		
		BlockingRpcCallback callback = new BlockingRpcCallback();
		if ( call.getService() != null ) {
			call.getService().callMethod(call.getMethodDesc(), call.getController(), call.getRequest(), callback);
			if ( !callback.isDone() ) {
				// this is only likely to come in here if another thread executes the callback than the
				// one calling callMethod.
				synchronized(callback) {
					while(!callback.isDone()) {
						try {
							callback.wait();
						} catch (InterruptedException e) {
							// if the service off-loaded running to a different thread, the currentThread
							// could be waiting here and be interrupted when cancel comes in.
							
							// we "consume" the thread's current thread's interrupt status and finish.
							break;
						}
					}
				}
				// callback may or may not have finished
			}
		} else {
			// handle blocking call service
			try {
				Message response = call.getBlockingService().callBlockingMethod(call.getMethodDesc(), call.getController(), call.getRequest());
				callback.run(response);
			} catch ( com.google.protobuf.ServiceException se ) {
				log.warn("BlockingService threw ServiceException.", se);
				callback.run(null);
				call.getController().setFailed("ServiceException");
			}
		}
		
		runningCalls.remove(Thread.currentThread());
		if ( Thread.interrupted() ) {
			//log clearing interrupted flag, which might have been set if we were interrupted
			//but not in a blocking wait before.
		}
		
		ServerRpcController controller = call.getController();
		if ( controller.isCanceled() ) {
			// we don't care if there was a response created or error, the
			// client is not interested anymore. Just to the notification
			if ( controller.getAndSetCancelCallbackNotified() ) {
				RpcCallback<Object> cancelCallback = controller.getCancelNotifyCallback();
				if ( cancelCallback != null ) {
					cancelCallback.run(null);
				}
			}
		} else {
			if ( !callback.isDone() ) {
				log.warn("Thread did not complete the RPC done callback.");
			}
			call.getExecutorCallback().onFinish(call.getController().getCorrelationId(), callback.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor#cancel(java.lang.Runnable)
	 */
	@Override
	public void cancel(Runnable executor) {
		Thread theadToCancel = (Thread)executor;
		PendingServerCallState call = runningCalls.get(theadToCancel);
		if ( call != null ) {
			
			// set the controller's cancel indicator
			call.getController().startCancel();
			
			// interrupt the thread running the task, to hopefully unblock any
			// IO wait.
			theadToCancel.interrupt();
			
			// the running task, still should finish by returning "null" to the RPC done
			// callback.
		}
	}

	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor#shutdown()
	 */
	@Override
	public void shutdown() {
		List<Thread> threadsRunning = new ArrayList<Thread>();
		threadsRunning.addAll(runningCalls.keySet());
		
		for( Thread t : threadsRunning ) {
			cancel( (Runnable)t );
		}
	}

	@Override
	public List<Runnable> shutdownNow() {
		throw new IllegalStateException();
	}

	@Override
	public boolean isShutdown() {
		return runningCalls.size() == 0;
	}

	@Override
	public boolean isTerminated() {
		throw new IllegalStateException();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		throw new IllegalStateException();
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		throw new IllegalStateException();
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		throw new IllegalStateException();
	}

	@Override
	public Future<?> submit(Runnable task) {
		throw new IllegalStateException();
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
			throws InterruptedException {
		throw new IllegalStateException();
	}

	@Override
	public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		throw new IllegalStateException();
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
			throws InterruptedException, ExecutionException {
		throw new IllegalStateException();
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
			long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		throw new IllegalStateException();
	}

	@Override
	public void execute(Runnable command) {
		throw new IllegalStateException();
	}
	
	
}
