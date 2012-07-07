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

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.codec.compression.ZlibWrapper;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.ssl.SslHandler;

import com.google.protobuf.ExtensionRegistry;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.RpcServer;
import com.googlecode.protobuf.pro.duplex.RpcServiceRegistry;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.handler.Handler;
import com.googlecode.protobuf.pro.duplex.handler.RpcClientHandler;
import com.googlecode.protobuf.pro.duplex.handler.RpcServerHandler;
import com.googlecode.protobuf.pro.duplex.handler.ServerConnectRequestHandler;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogger;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol;

public class DuplexTcpServerPipelineFactory implements
        ChannelPipelineFactory {

	private final PeerInfo serverInfo;
	private final RpcServiceRegistry rpcServiceRegistry;
	private final RpcClientRegistry rpcClientRegistry;
	private final RpcServerCallExecutor rpcServerCallExecutor;
	private final TcpConnectionEventListener eventListener;
	private final ServerConnectRequestHandler connectRequestHandler;
	private ExtensionRegistry wirepayloadExtensionRegistry;
	private final RpcLogger logger;
	
	private RpcSSLContext sslContext;
	
	public DuplexTcpServerPipelineFactory( PeerInfo serverInfo, RpcServiceRegistry rpcServiceRegistry, RpcClientRegistry rpcClientRegistry, RpcServerCallExecutor rpcServerCallExecutor, TcpConnectionEventListener eventListener, RpcLogger logger ) {
		if ( serverInfo == null ) {
			throw new IllegalArgumentException("serverInfo");
		}
		if ( rpcServiceRegistry == null ) {
			throw new IllegalArgumentException("rpcServiceRegistry");
		}
		if ( rpcClientRegistry == null ) {
			throw new IllegalArgumentException("rpcClientRegistry");
		}
		if ( rpcServerCallExecutor == null ) {
			throw new IllegalArgumentException("rpcServerCallExecutor");
		}
		if ( eventListener == null ) {
			throw new IllegalArgumentException("eventListener");
		}
		this.serverInfo = serverInfo;
		this.rpcServiceRegistry = rpcServiceRegistry;
		this.rpcClientRegistry = rpcClientRegistry;
		this.rpcServerCallExecutor = rpcServerCallExecutor;
		this.eventListener = eventListener;
		this.logger = logger;
		this.connectRequestHandler = new ServerConnectRequestHandler(serverInfo, rpcClientRegistry, this, logger);
	}
	
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = pipeline();

        if ( getSslContext() != null ) {
        	p.addLast(Handler.SSL, new SslHandler(getSslContext().createServerEngine()) );
        }
        
        p.addLast(Handler.FRAME_DECODER, new ProtobufVarint32FrameDecoder());
        p.addLast(Handler.PROTOBUF_DECODER, new ProtobufDecoder(DuplexProtocol.WirePayload.getDefaultInstance(), getWirepayloadExtensionRegistry()));

        p.addLast(Handler.FRAME_ENCODER, new ProtobufVarint32LengthFieldPrepender());
        p.addLast(Handler.PROTOBUF_ENCODER, new ProtobufEncoder());

        p.addLast(Handler.SERVER_CONNECT, connectRequestHandler); // one instance shared by all channels
        return p;
    }
    
    public RpcClientHandler completePipeline( RpcClient rpcClient ) {
    	ChannelPipeline p = rpcClient.getChannel().getPipeline();

    	if ( rpcClient.isCompression() ) {
	    	p.addBefore(Handler.FRAME_DECODER, Handler.DECOMPRESSOR, new ZlibEncoder(ZlibWrapper.GZIP));
	    	p.addAfter(Handler.DECOMPRESSOR, Handler.COMPRESSOR,  new ZlibDecoder(ZlibWrapper.GZIP));
    	}
    	
    	RpcClientHandler rpcClientHandler = new RpcClientHandler(rpcClient, eventListener);
    	p.replace(Handler.SERVER_CONNECT, Handler.RPC_CLIENT, rpcClientHandler);
    	
    	RpcServer rpcServer = new RpcServer(rpcClient, rpcServiceRegistry, rpcServerCallExecutor, logger); 
    	RpcServerHandler rpcServerHandler = new RpcServerHandler(rpcServer);
    	rpcServerHandler.setRpcClientRegistry(rpcClientRegistry);
    	p.addAfter(Handler.RPC_CLIENT, Handler.RPC_SERVER, rpcServerHandler);
    	return rpcClientHandler;
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
	 * @return the rpcServerCallExecutor
	 */
	public RpcServerCallExecutor getRpcServerCallExecutor() {
		return rpcServerCallExecutor;
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
	 * @return the wirepayloadExtensionRegistry
	 */
	public ExtensionRegistry getWirepayloadExtensionRegistry() {
		return wirepayloadExtensionRegistry;
	}

	/**
	 * @param wirepayloadExtensionRegistry the wirepayloadExtensionRegistry to set
	 */
	public void setWirepayloadExtensionRegistry(
			ExtensionRegistry wirepayloadExtensionRegistry) {
		this.wirepayloadExtensionRegistry = wirepayloadExtensionRegistry;
	}
    
}
