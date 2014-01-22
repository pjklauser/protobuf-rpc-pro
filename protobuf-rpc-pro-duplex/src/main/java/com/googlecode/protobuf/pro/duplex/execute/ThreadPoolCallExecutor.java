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

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;

/**
 * A ThreadPoolCallExecutor uses a pool of threads to handle the running of server side RPC calls.
 * This executor provides a cancellation facility where a running RPC call can be interrupted before
 * it finishes, because a RPC cancel which comes logically after the RPC call can be processed.
 * 
 * Thread separation between IO-Layer threads and threads calling the RPC methods is only needed if
 * the RPC method call is going to make an RPC call back to the SAME client who the call is servicing.
 * If other threads than the IO-Layer threads are calling the server's RPC clients, then the 
 * {@link SameTheadExecutor} is sufficient.
 * 
 * It is necessary to use separate threads to handle server RPC calls than the threads which serve
 * the RPC client calls, in order to avoid deadlock on the single Netty Channel.
 * 
 * You can choose bounds for number of Threads to service the calls, and the BlockingQueue size and
 * implementation through the choice of constructors. {@link java.util.concurrent.ThreadPoolExecutor}
 * discusses the choices here in detail.
 * 
 * By default we use {@link java.util.concurrent.ArrayBlockingQueue} to begin to throttle inbound calls
 * without building up a significant backlog ( introducing latency ). The ArrayBlockingQueue gives less
 * "Server Overload" issues due to thread scheduling situations ( thread not yet reading from queue when
 * the RPC inbound call is entered ).
 * 
 * Alternatively use {@link java.util.concurrent.SynchronousQueue} to avoid building up a backlog of queued
 * requests. If a RPC call comes in where a Thread is not ready to handle, you will receive
 * a "Server Overload" error.
 * 
 * @author Peter Klauser
 *
 */
public class ThreadPoolCallExecutor extends ThreadPoolExecutor implements RpcServerCallExecutor, RejectedExecutionHandler {

	private static Logger log = LoggerFactory.getLogger(ThreadPoolCallExecutor.class);

	Map<CallRunner,CallRunner> runningCalls = new ConcurrentHashMap<CallRunner, CallRunner>();
	
	public ThreadPoolCallExecutor(int corePoolSize, int maximumPoolSize) {
		this(corePoolSize, maximumPoolSize, 30, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(corePoolSize, false), new RenamingThreadFactoryProxy("rpc", Executors.defaultThreadFactory()) );
	}
	
	public ThreadPoolCallExecutor(int corePoolSize, int maximumPoolSize, ThreadFactory threadFactory) {
		this(corePoolSize, maximumPoolSize, 30, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(corePoolSize, false), threadFactory );
	}
	
	public ThreadPoolCallExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory);
		setRejectedExecutionHandler(this);
	}
	
	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor#execute(com.googlecode.protobuf.pro.duplex.execute.PendingServerCallState)
	 */
	@Override
	public void execute(PendingServerCallState call) {
		CallRunner runner = new CallRunner(call);
		runningCalls.put(runner, runner);
		call.setExecutor(runner);
		execute(runner);
	}

	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor#cancel(java.lang.Runnable)
	 */
	@Override
	public void cancel(Runnable executor) {
		CallRunner runner = runningCalls.remove(executor);
		if ( runner != null ) {
			PendingServerCallState call = runner.getCall();
			
			// set the controller's cancel indicator
			call.getController().startCancel();
			
			// interrupt the thread running the task, to hopefully unblock any
			// IO wait.
			Thread threadToCancel = runner.getRunningThread();
			if ( threadToCancel != null ) {
				threadToCancel.interrupt();
			}
		}
		// the running task, still should finish by returning "null" to the RPC done
		// callback.
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.ThreadPoolExecutor#beforeExecute(java.lang.Thread, java.lang.Runnable)
	 */
	@Override
	protected void beforeExecute(Thread t, Runnable r) {
		super.beforeExecute(t, r);
		
		CallRunner runner = runningCalls.get(r);
		if ( runner != null ) {
			runner.setRunningThread(t);
		}
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.ThreadPoolExecutor#afterExecute(java.lang.Runnable, java.lang.Throwable)
	 */
	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);

		if ( t != null ) {
			log.warn("RpcCallRunner threw uncaught runtime exception.", t);
		}
		
		CallRunner runner = runningCalls.remove(r);
		if ( runner != null ) {
			ServerRpcController controller = runner.getCall().getController();
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
				if ( !runner.getServiceCallback().isDone() ) {
					log.warn("RpcCallRunner did not finish RpcCall afterExecute. RpcCallRunner expected to complete calls, not offload them.");
				}
				runner.getCall().getExecutorCallback().onFinish(runner.getCall().getController().getCorrelationId(), runner.getServiceCallback().getMessage());
			}
			
		} else {
			if ( log.isDebugEnabled() ) {
				log.debug("Unable to find RpcCallRunner afterExecute - normal for a RpcCancel.");
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java.lang.Runnable, java.util.concurrent.ThreadPoolExecutor)
	 */
	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		CallRunner runner = runningCalls.remove(r);
		if ( runner != null ) {
			PendingServerCallState call = runner.getCall();
			call.getController().setFailed("Server Overload");
			call.getExecutorCallback().onFinish(call.getController().getCorrelationId(), null);
			// if we didn't even start to run, we don't have to worry about on cancel notify because
			// the cancel notification callback could never have been set!
		}
	}

	private static class CallRunner implements Runnable {

		private final PendingServerCallState call;
		private final BlockingRpcCallback serviceCallback = new BlockingRpcCallback();

		private Thread runningThread = null;
		
		public CallRunner( PendingServerCallState call ) {
			this.call = call;
		}
		
		@Override
		public void run() {
			if ( call.getController().isCanceled() ) {
				// canceled before start - return immediately
				return;
			}
			// due to buffering in the RpcServerCallExecutor, we may already be timed-out
			// so we dont want to process the call towards the RPC service.
			if( call.isTimeoutExceeded() ) {
				// set the controller's cancel indicator
				call.getController().startCancel();
				serviceCallback.run(null);
				return;
			}
			if ( call.getService() != null ) {
				call.getService().callMethod(call.getMethodDesc(), call.getController(), call.getRequest(), serviceCallback);
				if ( !serviceCallback.isDone() ) {
					// this is only likely to come in here if another thread executes the callback that the
					// one calling callMethod.
					synchronized(serviceCallback) {
						while(!serviceCallback.isDone()) {
							try {
								serviceCallback.wait();
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
				try {
					Message responseMessage = call.getBlockingService().callBlockingMethod(call.getMethodDesc(), call.getController(), call.getRequest());
					serviceCallback.run(responseMessage);
				} catch ( com.google.protobuf.ServiceException se ) {
					log.warn("BlockingService threw ServiceException.", se);
					serviceCallback.run(null);
					call.getController().setFailed(se.getMessage());
				}
			}
			if ( Thread.interrupted() ) {
				//log clearing interrupted flag, which might have been set if we were interrupted
				//but not in a blocking wait before.
			}
		}

		/**
		 * @param runningThread the runningThread to set
		 */
		public void setRunningThread(Thread runningThread) {
			this.runningThread = runningThread;
		}

		/**
		 * @return the call
		 */
		public PendingServerCallState getCall() {
			return call;
		}

		/**
		 * @return the runningThread
		 */
		public Thread getRunningThread() {
			return runningThread;
		}

		/**
		 * @return the serviceCallback
		 */
		public BlockingRpcCallback getServiceCallback() {
			return serviceCallback;
		}
		
	}

}
