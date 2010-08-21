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
package com.googlecode.protobuf.pro.duplex.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;

/**
 * The RpcClientConnectionRegistry keeps track of the RPC clients
 * that the RPC server knows about. 
 * 
 * It listens for TCP connection events and transforms them into 
 * RPC connection events which are fired towards the registered 
 * RpcConnectionEventListener.
 * 
 * @author Peter Klauser
 *
 */
public class RpcClientConnectionRegistry implements
		TcpConnectionEventListener {

	private static Log log = LogFactory.getLog(RpcClientConnectionRegistry.class);

	private Map<String,RpcClient> clientNameMap = new ConcurrentHashMap<String, RpcClient>();
	
	private RpcConnectionEventListener eventListener;
	
	public RpcClientConnectionRegistry() {
	}
	
	public RpcClientConnectionRegistry( RpcConnectionEventListener eventListener ) {
		this.eventListener = eventListener;
	}
	
	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.TcpConnectionEventListener#connectionClosed(com.googlecode.protobuf.pro.duplex.RpcClient)
	 */
	@Override
	public void connectionClosed(RpcClient client) {
		if ( log.isDebugEnabled() ) {
			log.debug("connectionClosed from " + client.getServerInfo());
		}
		RpcConnectionEventListener l = getEventListener();
		if ( l != null ) {
			l.connectionLost(client);
		}
		clientNameMap.put(client.getServerInfo().getName(), new RpcClient(null,null,client.getServerInfo()));
	}

	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.TcpConnectionEventListener#connectionOpened(com.googlecode.protobuf.pro.duplex.RpcClient)
	 */
	@Override
	public void connectionOpened(RpcClient client) {
		RpcConnectionEventListener l = getEventListener();
		PeerInfo peerInfo = client.getServerInfo();
		RpcClient existingClient = clientNameMap.get(peerInfo.getName());
		if ( existingClient == null ) {
			if ( log.isDebugEnabled() ) {
				log.debug("connectionOpened from " + peerInfo);
			}
			if ( l != null ) {
				l.connectionOpened(client);
			}
		} else {
			PeerInfo existingPeerInfo = existingClient.getServerInfo();
			if ( !existingPeerInfo.getPid().equals(client.getServerInfo().getPid())) {
				if ( log.isDebugEnabled() ) {
					log.debug("connectionChanged from " + existingPeerInfo + " to " + peerInfo);
				}
				if ( l != null ) {
					l.connectionChanged(client);
				}
			} else {
				if ( log.isDebugEnabled() ) {
					log.debug("connectionReestablished from " + peerInfo);
				}
				if ( l != null ) {
					l.connectionReestablished(client);
				}
			}
		}
	}

	/**
	 * @return the eventListener
	 */
	public RpcConnectionEventListener getEventListener() {
		return eventListener;
	}

	/**
	 * @param eventListener the eventListener to set
	 */
	public void setEventListener(RpcConnectionEventListener eventListener) {
		this.eventListener = eventListener;
	}

}
