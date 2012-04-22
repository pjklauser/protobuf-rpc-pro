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
package com.googlecode.protobuf.pro.stream.server;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.stream.PeerInfo;
import com.googlecode.protobuf.pro.stream.RpcSSLContext;
import com.googlecode.protobuf.pro.stream.logging.CategoryPerMessageTypeLogger;
import com.googlecode.protobuf.pro.stream.logging.StreamLogger;

public class StreamingServerBootstrap<E extends Message, F extends Message> extends ServerBootstrap {

	private static Log log = LogFactory.getLog(StreamingServerBootstrap.class);
	
	private final PeerInfo serverInfo;

	/**
	 * All Netty Channels created and bound by this StreamingServerBootstrap.
	 * 
	 * We keep hold of them to be able to do a clean shutdown.
	 */
	private ChannelGroup allChannels = new DefaultChannelGroup();
	
	public StreamingServerBootstrap(PeerInfo serverInfo, PullHandler<E> pullHandler, PushHandler<F> pushHandler, ChannelFactory channelFactory) {
		this(serverInfo, pullHandler, pushHandler, channelFactory, new CategoryPerMessageTypeLogger());
	}
	
	public StreamingServerBootstrap(PeerInfo serverInfo, PullHandler<E> pullHandler, PushHandler<F> pushHandler, ChannelFactory channelFactory, StreamLogger logger) {
		super(channelFactory);
		if ( serverInfo == null ) {
			throw new IllegalArgumentException("serverInfo");
		}
		this.serverInfo = serverInfo;
    	
		StreamingServerPipelineFactory<E,F> sf = new StreamingServerPipelineFactory<E,F>(serverInfo, pullHandler, pushHandler, logger); 
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
		if ( allChannels.remove(channel) ) {
			log.info("Closing IO Channel " + channel);
			channel.close();
		} else {
			log.warn("IO Channel " + channel + " not know by this Bootstrap.");
		}
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
	}

	@Override
	public String toString() {
		return "ServerBootstrap:"+serverInfo;
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
	@SuppressWarnings("unchecked")
	public RpcSSLContext getSslContext() {
		return ((StreamingServerPipelineFactory<E,F>)getPipelineFactory()).getSslContext();
	}

	/**
	 * @param sslContext the sslContext to set
	 */
	@SuppressWarnings("unchecked")
	public void setSslContext(RpcSSLContext sslContext) {
		((StreamingServerPipelineFactory<E,F>)getPipelineFactory()).setSslContext(sslContext);
	}

	/**
	 * @return the chunkSize
	 */
	@SuppressWarnings("unchecked")
	public int getChunkSize() {
		return ((StreamingServerPipelineFactory<E,F>)getPipelineFactory()).getChunkSize();
	}

	/**
	 * @param chunkSize the chunkSize to set
	 */
	@SuppressWarnings("unchecked")
	public void setChunkSize(int chunkSize) {
		((StreamingServerPipelineFactory<E,F>)getPipelineFactory()).setChunkSize(chunkSize);
	}

	/**
	 * @return the compress
	 */
	@SuppressWarnings("unchecked")
	public boolean isCompress() {
		return ((StreamingServerPipelineFactory<E,F>)getPipelineFactory()).isCompress();
	}

	/**
	 * @param compress the compress to set
	 */
	@SuppressWarnings("unchecked")
	public void setCompress(boolean compress) {
		((StreamingServerPipelineFactory<E,F>)getPipelineFactory()).setCompress(compress);
	}

}
