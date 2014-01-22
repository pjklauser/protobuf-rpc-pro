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
package com.googlecode.protobuf.pro.duplex.timeout;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcServer;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.RpcCancel;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.RpcError;

/**
 * A TimeoutExecutor uses a pool of threads to handle the timeout of client and server side RPC calls.
 * 
 * Timeout of an RPC call on a client side is equivalent to receiving an RPC error from the server side.
 * 
 * Timeout of an RPC call on the server side is equivalent to receiving an RPC cancel from the client side.
 * 
 * You can choose bounds for number of Threads to service the timeouts, and the BlockingQueue size and
 * implementation through the choice of constructors. {@link java.util.concurrent.ThreadPoolExecutor}
 * discusses the choices here in detail.
 * 
 * By default we use {@link java.util.concurrent.ArrayBlockingQueue} where we discard timeouts if the
 * array overflows. This is not a problem due to the re-checking nature of the TimeoutExecutor with all
 * the RpcClients and RpcServer of the Client or Server Bootstraps.
 * 
 * @author Peter Klauser
 *
 */
public class TimeoutExecutor extends ThreadPoolExecutor implements RpcTimeoutExecutor {

	private static Logger log = LoggerFactory.getLogger(TimeoutExecutor.class);

	public TimeoutExecutor(int corePoolSize, int maximumPoolSize) {
		this(corePoolSize, maximumPoolSize, 30, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(corePoolSize, false), new RenamingThreadFactoryProxy("timeout", Executors.defaultThreadFactory()) );
	}
	
	public TimeoutExecutor(int corePoolSize, int maximumPoolSize, ThreadFactory threadFactory) {
		this(corePoolSize, maximumPoolSize, 30, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(corePoolSize, false), threadFactory );
	}
	
	public TimeoutExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory);
		setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
	}
	
	@Override
	public void timeout(RpcServer rpcServer, RpcCancel rpcCancel) {
		ServerTimeoutRunner runner = new ServerTimeoutRunner(rpcServer, rpcCancel);
		execute(runner);
	}

	@Override
	public void timeout(RpcClient rpcClient, RpcError rpcError) {
		ClientTimeoutRunner runner = new ClientTimeoutRunner(rpcClient, rpcError);
		execute(runner);
	}

	private static class ServerTimeoutRunner implements Runnable {

		private final RpcCancel rpcCancel;
		private final RpcServer rpcServer;

		public ServerTimeoutRunner( RpcServer rpcServer, RpcCancel rpcCancel ) {
			this.rpcCancel = rpcCancel;
			this.rpcServer = rpcServer;
		}
		
		@Override
		public void run() {
			rpcServer.cancel(rpcCancel);
		}
	}


	private static class ClientTimeoutRunner implements Runnable {

		private final RpcError rpcError;
		private final RpcClient rpcClient;

		public ClientTimeoutRunner( RpcClient rpcClient, RpcError rpcError ) {
			this.rpcError = rpcError;
			this.rpcClient = rpcClient;
		}
		
		@Override
		public void run() {
			rpcClient.error(rpcError);
		}
	}


}
