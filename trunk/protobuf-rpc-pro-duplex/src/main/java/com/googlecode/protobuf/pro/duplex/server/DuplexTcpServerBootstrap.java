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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcServiceRegistry;
import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogger;

public class DuplexTcpServerBootstrap extends ServerBootstrap {

	private static Log log = LogFactory.getLog(DuplexTcpServerBootstrap.class);
	
	private List<TcpConnectionEventListener> connectionEventListeners = new ArrayList<TcpConnectionEventListener>();
	
	private final PeerInfo serverInfo;
	private final RpcServiceRegistry rpcServiceRegistry = new RpcServiceRegistry();
	private final RpcClientRegistry rpcClientRegistry = new RpcClientRegistry();
	private final RpcServerCallExecutor rpcServerCallExecutor;

	/**
	 * All Netty Channels created and bound by this DuplexTcpServerBootstrap.
	 * 
	 * We keep hold of them to be able to do a clean shutdown.
	 */
	private ChannelGroup allChannels = new DefaultChannelGroup();
	
	public DuplexTcpServerBootstrap(PeerInfo serverInfo, ChannelFactory channelFactory, RpcServerCallExecutor executor) {
		this(serverInfo, channelFactory, executor, new CategoryPerServiceLogger());
	}
	
	public DuplexTcpServerBootstrap(PeerInfo serverInfo, ChannelFactory channelFactory, RpcServerCallExecutor executor, RpcLogger logger) {
		super(channelFactory);
		if ( serverInfo == null ) {
			throw new IllegalArgumentException("serverInfo");
		}
		if ( executor == null ) {
			throw new IllegalArgumentException("executor");
		}
		this.serverInfo = serverInfo;
		this.rpcServerCallExecutor = executor;
		TcpConnectionEventListener informer = new TcpConnectionEventListener(){
			@Override
			public void connectionClosed(RpcClientChannel client) {
				for( TcpConnectionEventListener listener : getListenersCopy() ) {
					listener.connectionClosed(client);
				}
			}
			@Override
			public void connectionOpened(RpcClientChannel client) {
				for( TcpConnectionEventListener listener : getListenersCopy() ) {
					listener.connectionOpened(client);
				}
			}
		};
    	
		DuplexTcpServerPipelineFactory sf = new DuplexTcpServerPipelineFactory(serverInfo, rpcServiceRegistry, rpcClientRegistry, rpcServerCallExecutor, informer, logger); 
		setPipelineFactory(sf);
	}

	@Override
	public Channel bind( SocketAddress localAddress ) {
		if ( localAddress == null ) {
			throw new IllegalArgumentException("localAddress");
		}
		if ( localAddress instanceof InetSocketAddress ) {
			if ( serverInfo.getPort() != ((InetSocketAddress) localAddress).getPort() ) {
				log.warn("localAddress " + localAddress + " does not match serverInfo's port " + serverInfo.getPort());
			}
		}
		Channel c = super.bind(localAddress);
		
		allChannels.add(c);
		
		return c;
	}
	
	@Override
	public Channel bind() {
    	return bind( new InetSocketAddress(serverInfo.getPort()));
	}
	
	/**
	 * Unbind and close a Channel previously opened by this Bootstrap.
	 * 
	 * @param channel
	 */
	public void close( Channel channel ) {
		log.info("Closing IO Channel " + channel);
		channel.close().awaitUninterruptibly();
	}
	
	/* (non-Javadoc)
	 * @see org.jboss.netty.bootstrap.Bootstrap#releaseExternalResources()
	 */
	@Override
	public void releaseExternalResources() {
		log.debug("Closing all channels.");
		allChannels.close().awaitUninterruptibly();
		log.debug("Releasing IO-Layer external resources.");
		super.releaseExternalResources();
		log.debug("Releasing RPC Executor external resources.");
		this.rpcServerCallExecutor.shutdown();
	}

	@Override
	public String toString() {
		return "ServerBootstrap:"+serverInfo;
	}
	
	private List<TcpConnectionEventListener> getListenersCopy() {
		List<TcpConnectionEventListener> copy = new ArrayList<TcpConnectionEventListener>();
		copy.addAll(getConnectionEventListeners());
		
		return Collections.unmodifiableList(copy);
	}
	
	public void registerConnectionEventListener( TcpConnectionEventListener listener ) {
		getConnectionEventListeners().add(listener);
	}
	
	public void removeConnectionEventListener( TcpConnectionEventListener listener ) {
		getConnectionEventListeners().remove(listener);
	}
	
	/**
	 * @return the connectionEventListeners
	 */
	public List<TcpConnectionEventListener> getConnectionEventListeners() {
		if ( connectionEventListeners == null ) {
			return new ArrayList<TcpConnectionEventListener>(0);
		}
		return connectionEventListeners;
	}

	/**
	 * @param connectionEventListeners the connectionEventListeners to set
	 */
	public void setConnectionEventListeners(
			List<TcpConnectionEventListener> connectionEventListeners) {
		this.connectionEventListeners = connectionEventListeners;
	}

	/**
	 * @return the rpcServiceRegistry
	 */
	public RpcServiceRegistry getRpcServiceRegistry() {
		return rpcServiceRegistry;
	}

	/**
	 * @return the rpcClientRegistry
	 */
	public RpcClientRegistry getRpcClientRegistry() {
		return rpcClientRegistry;
	}

	/**
	 * @return the serverInfo
	 */
	public PeerInfo getServerInfo() {
		return serverInfo;
	}

	/**
	 * @return the sslContext
	 */
	public RpcSSLContext getSslContext() {
		return ((DuplexTcpServerPipelineFactory)getPipelineFactory()).getSslContext();
	}

	/**
	 * @param sslContext the sslContext to set
	 */
	public void setSslContext(RpcSSLContext sslContext) {
		((DuplexTcpServerPipelineFactory)getPipelineFactory()).setSslContext(sslContext);
	}

}
