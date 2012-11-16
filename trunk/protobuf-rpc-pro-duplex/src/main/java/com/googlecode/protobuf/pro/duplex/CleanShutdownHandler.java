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
package com.googlecode.protobuf.pro.duplex;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutExecutor;

/**
 * Registers a JVM shutdown hook to cleanly shutdown any
 * Client or Server Bootstraps, and TimeoutCheckers or TimeoutExecutors.
 * 
 * @author Peter Klauser
 *
 */
public class CleanShutdownHandler {

	private static Log log = LogFactory.getLog(CleanShutdownHandler.class);
	
	private List<DuplexTcpServerBootstrap> serverBootstraps = new LinkedList<DuplexTcpServerBootstrap>();
	private List<DuplexTcpClientBootstrap> clientBootstraps = new LinkedList<DuplexTcpClientBootstrap>();
	private List<RpcTimeoutChecker> timeoutCheckers = new LinkedList<RpcTimeoutChecker>();
	private List<RpcTimeoutExecutor> timeoutExecutors = new LinkedList<RpcTimeoutExecutor>();
	
	public CleanShutdownHandler() {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			
			@Override
			public void run() {
				log.debug("Releasing " + serverBootstraps.size() + " ServerBootstrap.");
				for( DuplexTcpServerBootstrap bootstrap : getServerBootstraps() ) {
					bootstrap.releaseExternalResources();
				}
				
				log.debug("Releasing " + clientBootstraps.size() + " ClientBootstrap.");
				for( DuplexTcpClientBootstrap bootstrap : getClientBootstraps() ) {
					bootstrap.releaseExternalResources();
				}

				log.debug("Releasing " + timeoutCheckers.size() + " RpcTimeoutChecker.");
				for( RpcTimeoutChecker timeoutChecker : getTimeoutCheckers() ) {
					timeoutChecker.shutdown();
				}

				log.debug("Releasing " + timeoutExecutors.size() + " RpcTimeoutExecutor.");
				for( RpcTimeoutExecutor timeoutExecutor : getTimeoutExecutors() ) {
					timeoutExecutor.shutdown();
				}
			}
		} ));
	}
	
	public void addResource( DuplexTcpClientBootstrap bootstrap ) {
		clientBootstraps.add(bootstrap);
	}
	
	public void removeResource( DuplexTcpClientBootstrap bootstrap ) {
		clientBootstraps.remove(bootstrap);
	}
	
	public void addResource( DuplexTcpServerBootstrap bootstrap ) {
		serverBootstraps.add(bootstrap);
	}
	
	public void removeResource( DuplexTcpServerBootstrap bootstrap ) {
		serverBootstraps.remove(bootstrap);
	}

	public void addResource( RpcTimeoutChecker checker ) {
		timeoutCheckers.add(checker);
	}
	
	public void removeResource( RpcTimeoutChecker checker ) {
		timeoutCheckers.remove(checker);
	}

	public void addResource( RpcTimeoutExecutor executor ) {
		timeoutExecutors.add(executor);
	}
	
	public void removeResource( RpcTimeoutExecutor executor ) {
		timeoutExecutors.remove(executor);
	}

	/**
	 * @return the serverBootstraps
	 */
	public List<DuplexTcpServerBootstrap> getServerBootstraps() {
		return serverBootstraps;
	}

	/**
	 * @param serverBootstraps the serverBootstraps to set
	 */
	public void setServerBootstraps(List<DuplexTcpServerBootstrap> serverBootstraps) {
		this.serverBootstraps = serverBootstraps;
	}

	/**
	 * @return the clientBootstraps
	 */
	public List<DuplexTcpClientBootstrap> getClientBootstraps() {
		return clientBootstraps;
	}

	/**
	 * @param clientBootstraps the clientBootstraps to set
	 */
	public void setClientBootstraps(List<DuplexTcpClientBootstrap> clientBootstraps) {
		this.clientBootstraps = clientBootstraps;
	}

	/**
	 * @return the timeoutCheckers
	 */
	public List<RpcTimeoutChecker> getTimeoutCheckers() {
		return timeoutCheckers;
	}

	/**
	 * @param timeoutCheckers the timeoutCheckers to set
	 */
	public void setTimeoutCheckers(List<RpcTimeoutChecker> timeoutCheckers) {
		this.timeoutCheckers = timeoutCheckers;
	}

	/**
	 * @return the timeoutExecutors
	 */
	public List<RpcTimeoutExecutor> getTimeoutExecutors() {
		return timeoutExecutors;
	}

	/**
	 * @param timeoutExecutors the timeoutExecutors to set
	 */
	public void setTimeoutExecutors(List<RpcTimeoutExecutor> timeoutExecutors) {
		this.timeoutExecutors = timeoutExecutors;
	}
}
