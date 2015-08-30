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

import io.netty.util.concurrent.EventExecutorGroup;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers a JVM shutdown hook to cleanly shutdown any
 * Client or Server Bootstraps, and TimeoutCheckers or TimeoutExecutors.
 * 
 * @author Peter Klauser, Gabriel Schlozer
 *
 */
public class CleanShutdownHandler {

	private static Logger log = LoggerFactory.getLogger(CleanShutdownHandler.class);
	
	private final ReentrantLock shutdownLOCK = new ReentrantLock();
	private List<EventExecutorGroup> bootstraps = new LinkedList<EventExecutorGroup>();
	private List<ExecutorService> executors = new LinkedList<ExecutorService>();
	
	public CleanShutdownHandler() {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			
			@Override
			public void run() {
				performShutdown(0);
			}
		} ));
	}
	
	public void addResource( EventExecutorGroup bootstrap ) {
		bootstraps.add(bootstrap);
	}
	
	public void addResource( ExecutorService executor ) {
		executors.add(executor);
	}
	
	public void removeResource( ExecutorService executor ) {
		executors.remove(executor);
	}

	/**
	 * @return the executors
	 */
	public List<ExecutorService> getExecutors() {
		return executors;
	}

	/**
	 * @param executors the executors to set
	 */
	public void setExecutors(List<ExecutorService> executors) {
		this.executors = executors;
	}

	/**
	 * @return the bootstraps
	 */
	public List<EventExecutorGroup> getBootstraps() {
		return bootstraps;
	}

	/**
	 * @param bootstraps the bootstraps to set
	 */
	public void setBootstraps(List<EventExecutorGroup> bootstraps) {
		this.bootstraps = bootstraps;
	}
	
	/**
	 * Shutdown all attached resources without waiting on the thread
	 */
	public void shutdown() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(createShutdown(0));
	}
	
	/**
	 * Shutdown all attached resources synchronously
	 * @param timeoutForEach time out for each resource independently (5 resources = max 5x value)
	 * @return Future which give global timeout result (true=no timeout, false=at least one timeout)
	 */
	public Future<Boolean> shutdownAwaiting(long timeoutForEach) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		return executor.submit(createShutdown(timeoutForEach));
	}
	
	private Callable<Boolean> createShutdown(final long timeoutForEach) {
		return new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				return performShutdown(timeoutForEach);
			}
		};
	}
	
	private boolean performShutdown(long timeoutForEach) {
		boolean success = true;
		shutdownLOCK.lock();
		try {
			log.debug("Releasing " + bootstraps.size() + " Client Bootstrap.");
			for( EventExecutorGroup bootstrap : getBootstraps() ) {
				bootstrap.shutdownGracefully();
			}
			
			log.debug("Releasing " + executors.size() + " Executors.");
			for( ExecutorService executor : getExecutors() ) {
				executor.shutdown();
			}
			
			if (timeoutForEach > 0) {
				for( EventExecutorGroup bootstrap : getBootstraps() ) {
					try {
						if (!bootstrap.awaitTermination(timeoutForEach, TimeUnit.MILLISECONDS)) {
							success = false;
						}
					}
					catch (InterruptedException e) {
						success = false;
					}
				}
				for( ExecutorService executor : getExecutors() ) {
					try {
						if (!executor.awaitTermination(timeoutForEach, TimeUnit.MILLISECONDS)) {
							success = false;
						}
					}
					catch (InterruptedException e) {
						success = false;
					}
				}
			}
		}
		finally {
			shutdownLOCK.unlock();
		}
		
		return success;
	}
}
