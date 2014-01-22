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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Service;

/**
 * The RpcServiceRegistry holds a reference to each RPC service 
 * implementation that can service calls. Optionally a message
 * ExtensionRegistry can be associated with the Service calls in
 * order for the RPC layer to transparently handle
 * protobuf messages with extensions. 
 * 
 * @author Peter Klauser
 *
 */
public class RpcServiceRegistry {
	
	private static Logger log = LoggerFactory.getLogger(RpcServiceRegistry.class);
	
	private Map<String, ServiceDescriptor> serviceNameMap = new HashMap<String, ServiceDescriptor>();
	
	public RpcServiceRegistry() {
	}
	
	/**
	 * Registers a Service implementation at an RPC server.
	 * 
	 * @param serviceImplementation
	 */
	public void registerService(Service serviceImplementation) {
		addService(true, serviceImplementation);
	}

	/**
	 * Registers a Service implementation at an RPC server.
	 * 
	 * @param allowTimeout whether to allow client timeouts to cause service cancellation.
	 * @param serviceImplementation
	 */
	public void registerService(boolean allowTimeout, Service serviceImplementation) {
		addService(allowTimeout, serviceImplementation);
	}

	/**
	 * Registers a BlockingService implementation at an RPC server.
	 * 
	 * @param serviceImplementation
	 */
	public void registerService(BlockingService serviceImplementation) {
		addService(true, serviceImplementation);
	}
	
	/**
	 * Registers a BlockingService implementation at an RPC server.
	 * 
	 * @deprecated use {@link #registerService(BlockingService)}
	 * @param serviceImplementation
	 */
	public void registerBlockingService(BlockingService serviceImplementation) {
		addService(true, serviceImplementation);
	}
	
	/**
	 * Registers a BlockingService implementation at an RPC server.
	 * 
	 * @param allowTimeout whether to allow client timeouts to cause service cancellation.
	 * @param serviceImplementation
	 */
	public void registerService(boolean allowTimeout, BlockingService serviceImplementation) {
		addService(allowTimeout, serviceImplementation);
	}

	/**
	 * Registers a BlockingService implementation at an RPC server.
	 * 
	 * @deprecated use {@link #addService(boolean, BlockingService)}
	 * @param allowTimeout whether to allow client timeouts to cause service cancellation.
	 * @param serviceImplementation
	 */
	public void registerBlockingService(boolean allowTimeout, BlockingService serviceImplementation) {
		addService(allowTimeout, serviceImplementation);
	}
	
	/**
	 * Removes a Service and it's corresponding ExtensionRegistry if
	 * one exists.
	 * 
	 * @param serviceImplementation
	 */
	public void removeService(Service serviceImplementation) {
		String serviceName = getServiceName(serviceImplementation.getDescriptorForType());
		if ( serviceNameMap.remove(serviceName) != null ) {
			
			log.info("Removed " + serviceName);
		}
	}

	public ServiceDescriptor resolveService(String serviceName) {
		ServiceDescriptor s = serviceNameMap.get(serviceName);
		if ( log.isDebugEnabled() ) {
			if ( s != null ) {
				log.debug("Resolved " + serviceName);
			} else {
				log.debug("Unable to resolve " + serviceName );
			}
		}
		return s;
	}

	private String addService(boolean allowTimeout, Service serviceImplementation) {
		String serviceName = getServiceName(serviceImplementation.getDescriptorForType());
		if ( serviceNameMap.containsKey(serviceName) ) {
			throw new IllegalStateException("Duplicate serviceName "+ serviceName);
		}
		serviceNameMap.put(serviceName, new ServiceDescriptor(allowTimeout, serviceImplementation));
		log.info("Registered NonBlocking " + serviceName +" allowTimeout="+(allowTimeout?"Y":"N"));
		
		return serviceName;
	}

	private String addService(boolean allowTimeout, BlockingService serviceImplementation) {
		String serviceName = getServiceName(serviceImplementation.getDescriptorForType());
		if ( serviceNameMap.containsKey(serviceName) ) {
			throw new IllegalStateException("Duplicate serviceName "+ serviceName);
		}
		serviceNameMap.put(serviceName, new ServiceDescriptor(allowTimeout, serviceImplementation));
		log.info("Registered Blocking " + serviceName + " allowTimeout="+(allowTimeout?"Y":"N"));
		
		return serviceName;
	}
	
	//Issue 29: use FQN for services to avoid problems with duplicate service names in different pkgs.
	private String getServiceName( Descriptors.ServiceDescriptor descriptor ) {
		return descriptor.getFullName();
	}
	
	public static class ServiceDescriptor {
		private final Service service;
		private final BlockingService blockingService;
		private final boolean allowTimeout;
		
		public ServiceDescriptor( boolean allowTimeout, BlockingService s ) {
			this.service = null;
			this.blockingService = s;
			this.allowTimeout = allowTimeout;
		}
		
		public ServiceDescriptor( boolean allowTimeout, Service s ) {
			this.service = s;
			this.blockingService = null;
			this.allowTimeout = allowTimeout;
		}

		/**
		 * @return the service
		 */
		public Service getService() {
			return service;
		}

		/**
		 * @return the blockingService
		 */
		public BlockingService getBlockingService() {
			return blockingService;
		}

		/**
		 * @return the allowTimeout
		 */
		public boolean isAllowTimeout() {
			return allowTimeout;
		}
	}
}
