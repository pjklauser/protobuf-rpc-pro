/**
 *   Copyright 2010-2013 Peter Klauser
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
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.bootstrap.Bootstrap;

/**
 * Registers a JVM shutdown hook to cleanly shutdown any
 * Client or Server Bootstraps, and TimeoutCheckers or TimeoutExecutors.
 * 
 * @author Peter Klauser
 *
 */
public class CleanShutdownHandler {

	private static Logger log = LoggerFactory.getLogger(CleanShutdownHandler.class);
	
	private List<Bootstrap> bootstraps = new LinkedList<Bootstrap>();
	private List<ExecutorService> executors = new LinkedList<ExecutorService>();
	
	public CleanShutdownHandler() {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			
			@Override
			public void run() {
				log.debug("Releasing " + bootstraps.size() + " Bootstrap.");
				for( Bootstrap bootstrap : getBootstraps() ) {
					bootstrap.releaseExternalResources();
				}
				
				log.debug("Releasing " + executors.size() + " Executors.");
				for( ExecutorService executor : getExecutors() ) {
					executor.shutdown();
				}
			}
		} ));
	}
	
	public void addResource( Bootstrap bootstrap ) {
		bootstraps.add(bootstrap);
	}
	
	public void removeResource( Bootstrap bootstrap ) {
		bootstraps.remove(bootstrap);
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
	public List<Bootstrap> getBootstraps() {
		return bootstraps;
	}

	/**
	 * @param bootstraps the bootstraps to set
	 */
	public void setBootstraps(List<Bootstrap> bootstraps) {
		this.bootstraps = bootstraps;
	}
}
