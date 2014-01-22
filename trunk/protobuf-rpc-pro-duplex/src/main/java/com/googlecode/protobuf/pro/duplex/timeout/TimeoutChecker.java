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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.server.RpcClientRegistry;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;

/**
 * 
 * @author Peter Klauser
 *
 */
public class TimeoutChecker extends ScheduledThreadPoolExecutor implements RpcTimeoutChecker {

	private static Logger log = LoggerFactory.getLogger(TimeoutChecker.class);

	private RpcTimeoutExecutor timeoutExecutor;
	private final int sleepTimeMs;
	
	private Map<RpcClientRegistry,ScheduledFuture<?>> registryMap = new HashMap<RpcClientRegistry,ScheduledFuture<?>>();
	
	public TimeoutChecker() {
		this(1000, 1, new RenamingThreadFactoryProxy("check", Executors.defaultThreadFactory()));
	}
	
	public TimeoutChecker(int sleepTimeMs, int corePoolSize, ThreadFactory threadFactory) {
		super(corePoolSize, threadFactory);
		this.sleepTimeMs = sleepTimeMs;
		setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
	}
	
	@Override
	public void startChecking(final RpcClientRegistry registry) {
		Runnable r = new Runnable() {
			public void run() {
				for( RpcClientChannel c : registry.getAllClients()) {
					if ( c instanceof RpcClient ) {
						RpcClient client = (RpcClient)c;
						
						if ( timeoutExecutor != null ) {
							if ( log.isDebugEnabled() ) {
								log.debug("Timeout checking " + client);
							}
							if ( client.getRpcServer() != null ) {
								client.getRpcServer().checkTimeouts(timeoutExecutor);
							}
							client.checkTimeouts(timeoutExecutor);
						} else {
							log.warn("No TimeoutExecutor defined.");
						}
					}
				}
			};
		};
		ScheduledFuture<?> future = scheduleWithFixedDelay(r, 0, sleepTimeMs, TimeUnit.MILLISECONDS);
		registryMap.put(registry,future);
	}


	@Override
	public void stopChecking(final RpcClientRegistry registry) {
		ScheduledFuture<?> future = registryMap.remove(registry);
		if ( future != null ) {
			future.cancel(true);
		}
	}
	
	/**
	 * @return the timeoutExecutor
	 */
	public RpcTimeoutExecutor getTimeoutExecutor() {
		return timeoutExecutor;
	}

	/**
	 * @param timeoutExecutor the timeoutExecutor to set
	 */
	public void setTimeoutExecutor(RpcTimeoutExecutor timeoutExecutor) {
		this.timeoutExecutor = timeoutExecutor;
	}

	/**
	 * @return the sleepTimeMs
	 */
	public int getSleepTimeMs() {
		return sleepTimeMs;
	}

}
