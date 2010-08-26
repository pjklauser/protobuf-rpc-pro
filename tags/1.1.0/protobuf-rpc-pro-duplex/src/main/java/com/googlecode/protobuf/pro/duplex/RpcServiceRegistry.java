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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Service;

/**
 * The RpcServiceRegistry holds a reference to each RPC service 
 * implementation that can service calls.
 * 
 * @author Peter Klauser
 *
 */
public class RpcServiceRegistry {
	
	private List<Service> serviceImplementations = new ArrayList<Service>();
	
	private Map<String, Service> serviceNameMap = new HashMap<String, Service>();
	
	public RpcServiceRegistry() {
	}
	
	public void registerService(Service serviceImplementation) {
		String serviceName = serviceImplementation.getDescriptorForType().getName();
		if ( serviceNameMap.containsKey(serviceName) ) {
			throw new IllegalStateException("Duplicate serviceName "+ serviceName);
		}
		serviceNameMap.put(serviceName, serviceImplementation);
		serviceImplementations.add(serviceImplementation);
	}

	public Service resolveService(String serviceName) {
		return serviceNameMap.get(serviceName);
	}

}
