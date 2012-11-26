/**
 *   Copyright 2010-2013 Peter Klauser
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

import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcServer;
import com.googlecode.protobuf.pro.duplex.handler.Handler;
import com.googlecode.protobuf.pro.duplex.handler.RpcClientHandler;
import com.googlecode.protobuf.pro.duplex.handler.RpcServerHandler;
import com.googlecode.protobuf.pro.duplex.handler.ServerConnectRequestHandler;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol;

public class DuplexTcpServerPipelineFactory implements ChannelPipelineFactory {

	private final DuplexTcpServerBootstrap bootstrap;
	private final ServerConnectRequestHandler connectRequestHandler;
	
	public DuplexTcpServerPipelineFactory( DuplexTcpServerBootstrap bootstrap ) {
		if ( bootstrap == null ) {
			throw new IllegalArgumentException("bootstrap");
		}
		if ( bootstrap.getServerInfo() == null ) {
			throw new IllegalArgumentException("serverInfo");
		}
		if ( bootstrap.getRpcServiceRegistry() == null ) {
			throw new IllegalArgumentException("rpcServiceRegistry");
		}
		if ( bootstrap.getRpcClientRegistry() == null ) {
			throw new IllegalArgumentException("rpcClientRegistry");
		}
		if ( bootstrap.getRpcServerCallExecutor() == null ) {
			throw new IllegalArgumentException("rpcServerCallExecutor");
		}
		this.bootstrap = bootstrap;
		this.connectRequestHandler = new ServerConnectRequestHandler(bootstrap, this);
	}
	
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = pipeline();

        if ( bootstrap.getSslContext() != null ) {
        	p.addLast(Handler.SSL, new SslHandler(bootstrap.getSslContext().createServerEngine()) );
        }
        
        p.addLast(Handler.FRAME_DECODER, new ProtobufVarint32FrameDecoder());
        p.addLast(Handler.PROTOBUF_DECODER, new ProtobufDecoder(DuplexProtocol.WirePayload.getDefaultInstance(), bootstrap.getWirelinePayloadExtensionRegistry()));

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
    	
		TcpConnectionEventListener informer = new TcpConnectionEventListener(){
			@Override
			public void connectionClosed(RpcClientChannel client) {
				for( TcpConnectionEventListener listener : bootstrap.getListenersCopy() ) {
					listener.connectionClosed(client);
				}
			}
			@Override
			public void connectionOpened(RpcClientChannel client) {
				for( TcpConnectionEventListener listener : bootstrap.getListenersCopy() ) {
					listener.connectionOpened(client);
				}
			}
		};
    	
    	RpcClientHandler rpcClientHandler = new RpcClientHandler(rpcClient, informer);
    	p.replace(Handler.SERVER_CONNECT, Handler.RPC_CLIENT, rpcClientHandler);
    	
    	RpcServer rpcServer = new RpcServer(rpcClient, bootstrap.getRpcServiceRegistry(), bootstrap.getRpcServerCallExecutor(), bootstrap.getLogger()); 
    	RpcServerHandler rpcServerHandler = new RpcServerHandler(rpcServer,bootstrap.getRpcClientRegistry());
    	p.addAfter(Handler.RPC_CLIENT, Handler.RPC_SERVER, rpcServerHandler);
    	return rpcClientHandler;
    }

}
