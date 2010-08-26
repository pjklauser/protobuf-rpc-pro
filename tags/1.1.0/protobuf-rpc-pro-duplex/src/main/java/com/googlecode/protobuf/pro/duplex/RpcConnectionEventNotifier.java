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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;

/**
 * The RpcConnectionEventNotifier keeps track of the know RPC peers
 * and issues notifications to the RpcConnectionEventListener. 
 * 
 * It listens for TCP connection events and transforms them into 
 * RPC connection events which are fired towards the registered 
 * RpcConnectionEventListener.
 * 
 * @author Peter Klauser
 *
 */
public class RpcConnectionEventNotifier implements
		TcpConnectionEventListener {

	private static Log log = LogFactory.getLog(RpcConnectionEventNotifier.class);

	private Map<String,RpcClientChannel> peerNameMap = new ConcurrentHashMap<String, RpcClientChannel>();
	
	private RpcConnectionEventListener eventListener;
	
	public RpcConnectionEventNotifier() {
	}
	
	public RpcConnectionEventNotifier( RpcConnectionEventListener eventListener ) {
		this.eventListener = eventListener;
	}
	
	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.TcpConnectionEventListener#connectionClosed(com.googlecode.protobuf.pro.duplex.RpcClientChannel)
	 */
	@Override
	public void connectionClosed(RpcClientChannel clientChannel) {
		if ( log.isDebugEnabled() ) {
			log.debug("connectionClosed from " + clientChannel.getPeerInfo());
		}
		RpcConnectionEventListener l = getEventListener();
		if ( l != null ) {
			l.connectionLost(clientChannel);
		}
		peerNameMap.put(clientChannel.getPeerInfo().getName(), new DetachedRpcClientChannel(clientChannel.getPeerInfo()));
	}

	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.TcpConnectionEventListener#connectionOpened(com.googlecode.protobuf.pro.duplex.RpcClientChannel)
	 */
	@Override
	public void connectionOpened(RpcClientChannel clientChannel) {
		RpcConnectionEventListener l = getEventListener();
		PeerInfo peerInfo = clientChannel.getPeerInfo();
		RpcClientChannel existingClient = peerNameMap.get(peerInfo.getName());
		if ( existingClient == null ) {
			if ( log.isDebugEnabled() ) {
				log.debug("connectionOpened from " + peerInfo);
			}
			if ( l != null ) {
				l.connectionOpened(clientChannel);
			}
		} else {
			PeerInfo existingPeerInfo = existingClient.getPeerInfo();
			if ( !existingPeerInfo.getPid().equals(clientChannel.getPeerInfo().getPid())) {
				if ( log.isDebugEnabled() ) {
					log.debug("connectionChanged from " + existingPeerInfo + " to " + peerInfo);
				}
				if ( l != null ) {
					l.connectionChanged(clientChannel);
				}
			} else {
				if ( log.isDebugEnabled() ) {
					log.debug("connectionReestablished from " + peerInfo);
				}
				if ( l != null ) {
					l.connectionReestablished(clientChannel);
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

	private static class DetachedRpcClientChannel implements RpcClientChannel {

		private PeerInfo peerInfo;
		
		public DetachedRpcClientChannel( PeerInfo peerInfo ) {
			this.peerInfo = peerInfo;
		}
		
		/* (non-Javadoc)
		 * @see com.google.protobuf.BlockingRpcChannel#callBlockingMethod(com.google.protobuf.Descriptors.MethodDescriptor, com.google.protobuf.RpcController, com.google.protobuf.Message, com.google.protobuf.Message)
		 */
		@Override
		public Message callBlockingMethod(MethodDescriptor method,
				RpcController controller, Message request,
				Message responsePrototype) throws ServiceException {
			throw new IllegalStateException("method not supported on detached RpcClientChannel.");
		}

		/* (non-Javadoc)
		 * @see com.google.protobuf.RpcChannel#callMethod(com.google.protobuf.Descriptors.MethodDescriptor, com.google.protobuf.RpcController, com.google.protobuf.Message, com.google.protobuf.Message, com.google.protobuf.RpcCallback)
		 */
		@Override
		public void callMethod(MethodDescriptor method,
				RpcController controller, Message request,
				Message responsePrototype, RpcCallback<Message> done) {
			throw new IllegalStateException("method not supported on detached RpcClientChannel.");
		}

		/* (non-Javadoc)
		 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#getPeerInfo()
		 */
		@Override
		public PeerInfo getPeerInfo() {
			return this.peerInfo;
		}

		/* (non-Javadoc)
		 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#newRpcController()
		 */
		@Override
		public RpcController newRpcController() {
			throw new IllegalStateException("method not supported on detached RpcClientChannel.");
		}

		/* (non-Javadoc)
		 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#close()
		 */
		@Override
		public void close() {
			throw new IllegalStateException("method not supported on detached RpcClientChannel.");
		}
		
	}
}
