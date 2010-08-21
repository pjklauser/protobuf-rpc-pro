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
package com.googlecode.protobuf.pro.duplex.util;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.Bootstrap;

import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;

/**
 * Registers a JVM shutdown hook to cleanly shutdown any
 * Client or Server Bootstraps.
 * 
 * @author Peter Klauser
 *
 */
public class CleanShutdownHandler {

	private static Log log = LogFactory.getLog(CleanShutdownHandler.class);
	
	private List<Bootstrap> enlistedResources = new LinkedList<Bootstrap>();
	
	public CleanShutdownHandler() {
		Runtime.getRuntime().addShutdownHook(new Thread(new ResourceReleaser()));
	}
	
	public void addResource( Bootstrap bootstrap ) {
		enlistedResources.add(bootstrap);
	}
	
	public void removeResource( Bootstrap bootstrap ) {
		enlistedResources.remove(bootstrap);
	}
	
	private class ResourceReleaser implements Runnable {

		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			log.debug("Releasing " + enlistedResources.size() + " Bootstrap.");
			
			for( Bootstrap bootstrap : enlistedResources ) {
				bootstrap.releaseExternalResources();
			}
		}
	}
}
