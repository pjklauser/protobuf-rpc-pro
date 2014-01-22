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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.ExtensionRegistry;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.RpcServer;
import com.googlecode.protobuf.pro.duplex.RpcServiceRegistry;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.SameThreadExecutor;
import com.googlecode.protobuf.pro.duplex.handler.Handler;
import com.googlecode.protobuf.pro.duplex.handler.RpcClientHandler;
import com.googlecode.protobuf.pro.duplex.handler.RpcServerHandler;
import com.googlecode.protobuf.pro.duplex.handler.ServerConnectRequestHandler;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogger;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol;

public class DuplexTcpServerPipelineFactory extends ChannelInitializer<Channel> {

	//TODO private static Logger log = LoggerFactory.getLogger(DuplexTcpServerPipelineFactory.class);
	
	private List<TcpConnectionEventListener> connectionEventListeners = new ArrayList<TcpConnectionEventListener>();
	
	private final PeerInfo serverInfo;
	private final RpcServiceRegistry rpcServiceRegistry = new RpcServiceRegistry();
	private final RpcClientRegistry rpcClientRegistry = new RpcClientRegistry();
	private RpcServerCallExecutor rpcServerCallExecutor = new SameThreadExecutor();
	private ExtensionRegistry extensionRegistry;
	private ExtensionRegistry wirelinePayloadExtensionRegistry;
	private RpcSSLContext sslContext;
	private RpcLogger logger = new CategoryPerServiceLogger();

	private final ServerConnectRequestHandler connectRequestHandler;
	
	public DuplexTcpServerPipelineFactory( PeerInfo serverInfo ) {
		if ( serverInfo == null ) {
			throw new IllegalArgumentException("serverInfo");
		}
		this.serverInfo = serverInfo;
		this.connectRequestHandler = new ServerConnectRequestHandler(this);
	}
	
	@Override
	protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();

        if ( getSslContext() != null ) {
        	p.addLast(Handler.SSL, new SslHandler(getSslContext().createServerEngine()) );
        }
        
        p.addLast(Handler.FRAME_DECODER, new ProtobufVarint32FrameDecoder());
        p.addLast(Handler.PROTOBUF_DECODER, new ProtobufDecoder(DuplexProtocol.WirePayload.getDefaultInstance(), getWirelinePayloadExtensionRegistry()));

        p.addLast(Handler.FRAME_ENCODER, new ProtobufVarint32LengthFieldPrepender());
        p.addLast(Handler.PROTOBUF_ENCODER, new ProtobufEncoder());

        p.addLast(Handler.SERVER_CONNECT, connectRequestHandler); // one instance shared by all channels
	}
    
    public RpcClientHandler completePipeline( RpcClient rpcClient ) {
    	ChannelPipeline p = rpcClient.getChannel().pipeline();

    	if ( rpcClient.isCompression() ) {
	    	p.addBefore(Handler.FRAME_DECODER, Handler.COMPRESSOR, ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
	    	p.addAfter(Handler.COMPRESSOR, Handler.DECOMPRESSOR, ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
    	}
    	
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
    	
    	RpcClientHandler rpcClientHandler = new RpcClientHandler(rpcClient, informer);
    	p.replace(Handler.SERVER_CONNECT, Handler.RPC_CLIENT, rpcClientHandler);
    	
    	RpcServer rpcServer = new RpcServer(rpcClient, getRpcServiceRegistry(), getRpcServerCallExecutor(), getLogger()); 
    	RpcServerHandler rpcServerHandler = new RpcServerHandler(rpcServer,getRpcClientRegistry());
    	p.addAfter(Handler.RPC_CLIENT, Handler.RPC_SERVER, rpcServerHandler);
    	return rpcClientHandler;
    }


	@Override
	public String toString() {
		return "DuplexTcpServerPipelineFactory:"+serverInfo;
	}
	

	public List<TcpConnectionEventListener> getListenersCopy() {
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
		return sslContext;
	}

	/**
	 * @param sslContext the sslContext to set
	 */
	public void setSslContext(RpcSSLContext sslContext) {
		this.sslContext = sslContext;
	}

	/**
	 * @return the registered extension registry.
	 */
	public ExtensionRegistry getExtensionRegistry() {
		return extensionRegistry;
	}
	
	/**
	 * Set the extension registry.
	 * 
	 * @param extensionRegistry
	 */
	public void setExtensionRegistry( ExtensionRegistry extensionRegistry ) {
		this.extensionRegistry = extensionRegistry;
	}

	/**
	 * @return the registered WirelinePayload's extension registry.
	 */
	public ExtensionRegistry getWirelinePayloadExtensionRegistry() {
		return wirelinePayloadExtensionRegistry;
	}
	
	/**
	 * Set the WirelinePayload's extension registry.
	 * 
	 * @param extensionRegistry
	 */
	public void setWirelinePayloadExtensionRegistry( ExtensionRegistry wirelinePayloadExtensionRegistry ) {
		this.wirelinePayloadExtensionRegistry = wirelinePayloadExtensionRegistry;
	}

	/**
	 * @return the logger
	 */
	public RpcLogger getLogger() {
		return logger;
	}

	/**
	 * @param logger the logger to set
	 */
	public void setLogger(RpcLogger logger) {
		this.logger = logger;
	}

	/**
	 * @return the rpcServerCallExecutor
	 */
	public RpcServerCallExecutor getRpcServerCallExecutor() {
		return rpcServerCallExecutor;
	}

	/**
	 * @param rpcServerCallExecutor the rpcServerCallExecutor to set
	 */
	public void setRpcServerCallExecutor(RpcServerCallExecutor rpcServerCallExecutor) {
		this.rpcServerCallExecutor = rpcServerCallExecutor;
	}
}
