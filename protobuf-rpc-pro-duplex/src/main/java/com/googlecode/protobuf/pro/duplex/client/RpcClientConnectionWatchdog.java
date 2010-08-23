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
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;

/**
 * The idea is for someone to try to keep a connection alive
 * 
 * TODO investigate using a "DelayQueue" for this.
 * 
 * @author Peter Klauser
 *
 */
public class RpcClientConnectionWatchdog implements RpcConnectionEventListener {
	
    private static Log log = LogFactory.getLog(RpcClientConnectionWatchdog.class);
	
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
		RpcClientChannel rpcClientChannel = null;
		
		public RetryState( RpcClientChannel clientChannel ) {
			rpcClientChannel = clientChannel;
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
			RpcClientChannel disconnectedClient = state.rpcClientChannel;
			
			PeerInfo serverInfo = disconnectedClient.getPeerInfo();
			
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
	public void connectionLost(RpcClientChannel clientChannel) {
		addRetryState(new RetryState(clientChannel));
		trigger();
	}

	@Override
	public void connectionOpened(RpcClientChannel clientChannel) {
	}

	@Override
	public void connectionReestablished(RpcClientChannel clientChannel) {
	}

	@Override
	public void connectionChanged(RpcClientChannel clientChannel) {
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
