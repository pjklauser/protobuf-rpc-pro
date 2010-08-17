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
package com.googlecode.protobuf.pro.duplex.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;

public class RpcClientConnectionWatchdog implements RpcConnectionEventListener {
	
    private static Log log = LogFactory.getLog(RpcClientConnectionWatchdog.class);
	//private static final Logger log = Logger.getLogger(RpcClientConnectionWatchdog.class);
	
	private List<RetryState> watchedClients = new ArrayList<RetryState>();
	
	private final DuplexTcpClientBootstrap clientBootstrap;
	
	private Thread thread;
	private WatchdogThread watchdogThread;
	private long retryIntervalMillis = 10000;
	
	public RpcClientConnectionWatchdog(DuplexTcpClientBootstrap clientBootstrap)  {
		this.clientBootstrap = clientBootstrap;
	}

	public void start() {
		watchdogThread = new WatchdogThread(this);
		thread = new Thread(watchdogThread);
		thread.setDaemon(true);
		thread.start();
	}
	
	public void stop() {
		if ( watchdogThread != null ) {
			watchdogThread.finish();
		}
		watchdogThread = null;
		if ( thread != null ) {
			thread = null;
		}
	}
	
	private static class RetryState {
		long lastRetryTime = 0;
		RpcClient rpcClient = null;
		
		public RetryState( RpcClient client ) {
			rpcClient = client;
		}
		
	}
	
	boolean isRetryableNow( RetryState state ) {
		if ( state.lastRetryTime == 0 ) {
			return true;
		}
		if ( state.lastRetryTime + getRetryIntervalMillis() <= System.currentTimeMillis() ) {
			return true;
		}
		return false;
	}
	
	List<RetryState> getRetryableStates() {
		List<RetryState> retryList = new ArrayList<RetryState>();
		synchronized( watchedClients ) {
			for( RetryState state : watchedClients ) {
				if ( isRetryableNow(state)) {
					retryList.add(state);
				}
			}
		}
		return retryList;
	}
	
	void addRetryState( RetryState state ) {
		synchronized (watchedClients) {
			watchedClients.add(state);
		}
	}
	
	void removeRetryState( RetryState state ) {
		synchronized (watchedClients) {
			watchedClients.remove(state);
		}
	}
	
	void trigger() {
		WatchdogThread wt = watchdogThread;
		if ( wt != null) {
			wt.trigger();
		}
	}
	
	public static class WatchdogThread implements Runnable {

		private boolean stopped = false;
		private RpcClientConnectionWatchdog watchdog;
		private Object triggerSyncObject = new Object();
		
		public WatchdogThread( RpcClientConnectionWatchdog watchdog ) {
			this.watchdog = watchdog;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			while( !stopped ) {
				do {
					List<RetryState> retryableStates = watchdog.getRetryableStates();
					if ( retryableStates.size() == 0 ) {
						// if we've got nothing to do we just wait for a bit before checking again.
						// if something comes in ( ie. connectionLost ) then start retrying immediately.
						try {
							synchronized (triggerSyncObject) {
								triggerSyncObject.wait(watchdog.getRetryIntervalMillis());
							}
						} catch ( InterruptedException e ) {
							
						}
					} else {
						for( RetryState state : retryableStates ) {
							doRetry(state);
						}
					}
				} while( !stopped );
			}
		}
		
		public void finish() {
			this.stopped = true;
			trigger();
		}
		
		public void trigger() {
			synchronized( triggerSyncObject ) {
				triggerSyncObject.notifyAll();
			}
		}
		
		void doRetry(RetryState state) {
			RpcClient disconnectedClient = state.rpcClient;
			
			PeerInfo serverInfo = disconnectedClient.getServerInfo();
			
			state.lastRetryTime = System.currentTimeMillis();
			try {
				log.info("Retry connecting " + serverInfo );
				watchdog.getClientBootstrap().peerWith(serverInfo);
				log.info("Retry succeeded " + serverInfo );
				watchdog.removeRetryState(state);
			} catch ( IOException e ) {
				// leave it in the list
				log.info("Retry failed " + serverInfo, e );
			}
		}
	}
	
	@Override
	public void connectionLost(RpcClient client) {
		addRetryState(new RetryState(client));
		trigger();
	}

	@Override
	public void connectionOpened(RpcClient client) {
	}

	@Override
	public void connectionReestablished(RpcClient client) {
	}

	@Override
	public void connectionChanged(RpcClient client) {
	}

	/**
	 * @return the retryIntervalMillis
	 */
	public long getRetryIntervalMillis() {
		return retryIntervalMillis;
	}

	/**
	 * @param retryIntervalMillis the retryIntervalMillis to set
	 */
	public void setRetryIntervalMillis(long retryIntervalMillis) {
		this.retryIntervalMillis = retryIntervalMillis;
	}

	/**
	 * @return the clientBootstrap
	 */
	public DuplexTcpClientBootstrap getClientBootstrap() {
		return clientBootstrap;
	}
	
}
