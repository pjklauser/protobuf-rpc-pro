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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.BlockingService;
import com.google.protobuf.ExtensionRegistry;
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
	
	private static Log log = LogFactory.getLog(RpcServiceRegistry.class);
	
	private Map<String, ServiceDescriptor> serviceNameMap = new HashMap<String, ServiceDescriptor>();
	
	public RpcServiceRegistry() {
	}
	
	/**
	 * Registers a Service implementation at an RPC server without an message
	 * ExtensionRegistry.
	 * 
	 * @param serviceImplementation
	 */
	public void registerService(Service serviceImplementation) {
		addService(true, serviceImplementation, null);
	}

	/**
	 * Registers a Service implementation at an RPC server without an message
	 * ExtensionRegistry.
	 * 
	 * @param allowTimeout whether to allow client timeouts to cause service cancellation.
	 * @param serviceImplementation
	 */
	public void registerService(boolean allowTimeout, Service serviceImplementation) {
		addService(allowTimeout, serviceImplementation, null);
	}

	/**
	 * Registers a Service implementation at an RPC server with a message
	 * ExtensionRegistry.
	 * 
	 * @param allowTimeout whether to allow client timeouts to cause service cancellation.
	 * @param serviceImplementation
	 * @param extensionRegistry
	 */
	public void registerService(boolean allowTimeout, Service serviceImplementation, ExtensionRegistry extensionRegistry ) {
		if ( extensionRegistry == null ) {
			throw new IllegalArgumentException("Missing extensionRegistry");
		}
		addService(allowTimeout, serviceImplementation, extensionRegistry);
	}

	/**
	 * Registers a BlockingService implementation at an RPC server without an message
	 * ExtensionRegistry.
	 * 
	 * @param serviceImplementation
	 */
	public void registerBlockingService(BlockingService serviceImplementation) {
		addService(true, serviceImplementation, null);
	}
	
	/**
	 * Registers a BlockingService implementation at an RPC server without an message
	 * ExtensionRegistry.
	 * 
	 * @param allowTimeout whether to allow client timeouts to cause service cancellation.
	 * @param serviceImplementation
	 */
	public void registerBlockingService(boolean allowTimeout, BlockingService serviceImplementation) {
		addService(allowTimeout, serviceImplementation, null);
	}

	/**
	 * Registers a BlockingService implementation at an RPC server with a message
	 * ExtensionRegistry.
	 * 
	 * @param allowTimeout whether to allow client timeouts to cause service cancellation.
	 * @param serviceImplementation
	 * @param extensionRegistry
	 */
	public void registerService(boolean allowTimeout, BlockingService serviceImplementation, ExtensionRegistry extensionRegistry ) {
		if ( extensionRegistry == null ) {
			throw new IllegalArgumentException("Missing extensionRegistry");
		}
		addService(allowTimeout, serviceImplementation, extensionRegistry);
	}

	/**
	 * Removes a Service and it's corresponding ExtensionRegistry if
	 * one exists.
	 * 
	 * @param serviceImplementation
	 */
	public void removeService(Service serviceImplementation) {
		String serviceName = serviceImplementation.getDescriptorForType().getName();
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

	private String addService(boolean allowTimeout, Service serviceImplementation, ExtensionRegistry er) {
		String serviceName = serviceImplementation.getDescriptorForType().getName();
		if ( serviceNameMap.containsKey(serviceName) ) {
			throw new IllegalStateException("Duplicate serviceName "+ serviceName);
		}
		serviceNameMap.put(serviceName, new ServiceDescriptor(allowTimeout, serviceImplementation, er));
		log.info("Registered NonBlocking " + serviceName + (er!=null?" with ExtensionRegistry":"")+" allowTimeout="+(allowTimeout?"Y":"N"));
		
		return serviceName;
	}

	private String addService(boolean allowTimeout, BlockingService serviceImplementation, ExtensionRegistry er) {
		String serviceName = serviceImplementation.getDescriptorForType().getName();
		if ( serviceNameMap.containsKey(serviceName) ) {
			throw new IllegalStateException("Duplicate serviceName "+ serviceName);
		}
		serviceNameMap.put(serviceName, new ServiceDescriptor(allowTimeout, serviceImplementation, er));
		log.info("Registered Blocking " + serviceName + (er!=null?" with ExtensionRegistry":"")+" allowTimeout="+(allowTimeout?"Y":"N"));
		
		return serviceName;
	}
	
	public static class ServiceDescriptor {
		private final Service service;
		private final BlockingService blockingService;
		private final ExtensionRegistry extensionRegistry;
		private final boolean allowTimeout;
		
		public ServiceDescriptor( boolean allowTimeout, BlockingService s, ExtensionRegistry er ) {
			this.service = null;
			this.blockingService = s;
			this.extensionRegistry = er;
			this.allowTimeout = allowTimeout;
		}
		
		public ServiceDescriptor( boolean allowTimeout, Service s, ExtensionRegistry er ) {
			this.service = s;
			this.blockingService = null;
			this.extensionRegistry = er;
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
		 * @return the extensionRegistry
		 */
		public ExtensionRegistry getExtensionRegistry() {
			return extensionRegistry;
		}

		/**
		 * @return the allowTimeout
		 */
		public boolean isAllowTimeout() {
			return allowTimeout;
		}
	}
}
