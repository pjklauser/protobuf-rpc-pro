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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * {@link RpcConnectionEventListener}s.
 * 
 * @author Peter Klauser
 *
 */
public class RpcConnectionEventNotifier implements
		TcpConnectionEventListener {

	private static Logger log = LoggerFactory.getLogger(RpcConnectionEventNotifier.class);

	private Map<String,RpcClientChannel> peerNameMap = new ConcurrentHashMap<String, RpcClientChannel>();
	
	private List<RpcConnectionEventListener> eventListeners = new ArrayList<RpcConnectionEventListener>();
	
	public RpcConnectionEventNotifier() {
	}
	
	public RpcConnectionEventNotifier( RpcConnectionEventListener eventListener ) {
		this.eventListeners.add(eventListener);
	}
	
	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.TcpConnectionEventListener#connectionClosed(com.googlecode.protobuf.pro.duplex.RpcClientChannel)
	 */
	@Override
	public void connectionClosed(RpcClientChannel clientChannel) {
		if ( log.isDebugEnabled() ) {
			log.debug("connectionClosed from " + clientChannel.getPeerInfo());
		}
		List<RpcConnectionEventListener> ls = getEventListeners();
		for( RpcConnectionEventListener l : ls ) {
			l.connectionLost(clientChannel);
		}
		peerNameMap.put(clientChannel.getPeerInfo().getName(), new DetachedRpcClientChannel(clientChannel.getPeerInfo()));
	}

	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.duplex.TcpConnectionEventListener#connectionOpened(com.googlecode.protobuf.pro.duplex.RpcClientChannel)
	 */
	@Override
	public void connectionOpened(RpcClientChannel clientChannel) {
		List<RpcConnectionEventListener> ls = getEventListeners();
		PeerInfo peerInfo = clientChannel.getPeerInfo();
		RpcClientChannel existingClient = peerNameMap.get(peerInfo.getName());
		if ( existingClient == null ) {
			if ( log.isDebugEnabled() ) {
				log.debug("connectionOpened from " + peerInfo);
			}
			for( RpcConnectionEventListener l : ls ) {
				l.connectionOpened(clientChannel);
			}
		} else {
			PeerInfo existingPeerInfo = existingClient.getPeerInfo();
			if ( !existingPeerInfo.getPid().equals(clientChannel.getPeerInfo().getPid())) {
				if ( log.isDebugEnabled() ) {
					log.debug("connectionChanged from " + existingPeerInfo + " to " + peerInfo);
				}
				for( RpcConnectionEventListener l : ls ) {
					l.connectionChanged(clientChannel);
				}
			} else {
				if ( log.isDebugEnabled() ) {
					log.debug("connectionReestablished from " + peerInfo);
				}
				for( RpcConnectionEventListener l : ls ) {
					l.connectionReestablished(clientChannel);
				}
			}
		}
	}

	/**
	 * @return the eventListeners
	 */
	public List<RpcConnectionEventListener> getEventListeners() {
		List<RpcConnectionEventListener> l = new ArrayList<RpcConnectionEventListener>();
		l.addAll(eventListeners);
		return Collections.unmodifiableList(l);
	}

	/**
	 * Set the eventListener as the ONLY event listener. 
	 * Equivalent to {@link #clearEventListeners()} and then
	 * {@link #addEventListener(RpcConnectionEventListener)}
	 * 
	 * @param eventListener the eventListener to add
	 */
	public void setEventListener(RpcConnectionEventListener eventListener) {
		clearEventListeners();
		addEventListener(eventListener);
	}

	/**
	 * @param eventListener the eventListener to add
	 */
	public void addEventListener(RpcConnectionEventListener eventListener) {
		this.eventListeners.add(eventListener);
	}

	/**
	 * @param eventListener the eventListener to remove
	 */
	public void removeEventListener(RpcConnectionEventListener eventListener) {
		this.eventListeners.remove(eventListener);
	}

	/**
	 * Clear the eventListeners.
	 */
	public void clearEventListeners() {
		this.eventListeners.clear();
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
		public ClientRpcController newRpcController() {
			throw new IllegalStateException("method not supported on detached RpcClientChannel.");
		}

		/* (non-Javadoc)
		 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#close()
		 */
		@Override
		public void close() {
			throw new IllegalStateException("method not supported on detached RpcClientChannel.");
		}

		@Override
		public ChannelPipeline getPipeline() {
			throw new IllegalStateException("method not supported on detached RpcClientChannel.");
		}

		/* (non-Javadoc)
		 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#setOobMessageCallback(com.google.protobuf.Message, com.google.protobuf.RpcCallback)
		 */
		@Override
		public void setOobMessageCallback(Message responsePrototype,
				RpcCallback<? extends Message> onMessage) {
			throw new IllegalStateException("method not supported on detached RpcClientChannel.");
		}

		/* (non-Javadoc)
		 * @see com.googlecode.protobuf.pro.duplex.RpcClientChannel#sendUnsolicitedMessage(com.google.protobuf.Message)
		 */
		@Override
		public ChannelFuture sendOobMessage(Message message) {
			throw new IllegalStateException("method not supported on detached RpcClientChannel.");
		}
		
	}
}
