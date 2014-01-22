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
package com.googlecode.protobuf.pro.duplex.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;

/**
 * An RpcClientRegistry keeps an account of all connected RpcClients of an RpcServer.
 * 
 * Note: on the server side, an RpcClient's clientInfo is the server itself, and the
 * serverInfo represents the client.
 * 
 * @author Peter Klauser
 *
 */
public class RpcClientRegistry {
	
	private static Logger log = LoggerFactory.getLogger(RpcClientRegistry.class);
	
	private Map<String, RpcClient> clientNameMap = new ConcurrentHashMap<String, RpcClient>();
	
	public RpcClientRegistry() {
	}

	/**
	 * A convenience method for a server to get a list of all attached RpcClients.
	 * 
	 * An alternative is that the server keeps its own records by using 
	 * {@link RpcConnectionEventListener} functions.
	 * 
	 * @return an unmodifyable List of connected RpcClients.
	 */
	public List<RpcClientChannel> getAllClients() {
		List<RpcClientChannel> result = new ArrayList<RpcClientChannel>();
		result.addAll(clientNameMap.values());
		return Collections.unmodifiableList(result);
	}
	
	/**
	 * Attempt to register an RpcClient which has newly connected, during
	 * connection handshake. If the client is already connected, return false.
	 * 
	 * @param rpcClient
	 * @return true if registration is successful, false if already connected.
	 */
	public boolean registerRpcClient( RpcClient rpcClient ) {
		RpcClient existingClient = clientNameMap.get(rpcClient.getChannelName());
		if ( existingClient == null ) {
			clientNameMap.put(rpcClient.getChannelName(), rpcClient);
			return true;
		}
		if ( log.isDebugEnabled() ) {
			log.debug("RpcClient " + rpcClient.getChannelName() + " is already registered with " + existingClient.getServerInfo());
		}
		return false;
	}
	
	/**
	 * Remove the RpcClient from the registry, at connection close time.
	 * 
	 * @param rpcClient
	 */
	public void removeRpcClient( RpcClient rpcClient ) {
		clientNameMap.remove(rpcClient.getChannelName());
	}
}
