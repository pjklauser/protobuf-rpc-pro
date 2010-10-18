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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
	
	private Map<String, Service> serviceNameMap = new HashMap<String, Service>();
	private Map<String, ExtensionRegistry> extensionRegistryNameMap = new HashMap<String, ExtensionRegistry>();
	
	public RpcServiceRegistry() {
	}
	
	/**
	 * Registers a Service implementation at an RPC server without an message
	 * ExtensionRegistry.
	 * 
	 * @param serviceImplementation
	 */
	public void registerService(Service serviceImplementation) {
		String serviceName = addService(serviceImplementation);
		log.info("Registered " + serviceName);
	}

	/**
	 * Registers a Service implementation at an RPC server with a message
	 * ExtensionRegistry.
	 * 
	 * @param serviceImplementation
	 * @param extensionRegistry
	 */
	public void registerService(Service serviceImplementation, ExtensionRegistry extensionRegistry ) {
		if ( extensionRegistry == null ) {
			throw new IllegalArgumentException("Missing extensionRegistry");
		}
		String serviceName = addService(serviceImplementation);
		extensionRegistryNameMap.put(serviceName, extensionRegistry);
		log.info("Registered " + serviceName + " with ExtensionRegistry");
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
			
			if ( extensionRegistryNameMap.remove(serviceName) != null ) {
				log.info("Removed " + serviceName + " with ExtensionRegistry");
			} else {
				log.info("Removed " + serviceName);
			}
		}
	}

	public Service resolveService(String serviceName) {
		Service s = serviceNameMap.get(serviceName);
		if ( log.isDebugEnabled() ) {
			if ( s != null ) {
				log.debug("Resolved " + serviceName);
			} else {
				log.debug("Unable to resolve " + serviceName );
			}
		}
		return s;
	}

	public ExtensionRegistry resolveExtensionRegistry(String serviceName) {
		ExtensionRegistry er = extensionRegistryNameMap.get(serviceName);
		if ( log.isDebugEnabled() ) {
			if ( er != null ) {
				log.debug("Resolved ExtensionRegistry " + serviceName);
			}
		}
		return er;
	}

	private String addService(Service serviceImplementation) {
		String serviceName = serviceImplementation.getDescriptorForType().getName();
		if ( serviceNameMap.containsKey(serviceName) ) {
			throw new IllegalStateException("Duplicate serviceName "+ serviceName);
		}
		serviceNameMap.put(serviceName, serviceImplementation);
		
		return serviceName;
	}

}
