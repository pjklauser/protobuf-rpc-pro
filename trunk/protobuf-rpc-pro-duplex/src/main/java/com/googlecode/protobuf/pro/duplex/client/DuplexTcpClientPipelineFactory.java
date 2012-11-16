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
package com.googlecode.protobuf.pro.duplex.client;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.ssl.SslHandler;

import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.handler.ClientConnectResponseHandler;
import com.googlecode.protobuf.pro.duplex.handler.Handler;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol;

public class DuplexTcpClientPipelineFactory implements
        ChannelPipelineFactory {

	private final DuplexTcpClientBootstrap bootstrap;
	
    public DuplexTcpClientPipelineFactory( DuplexTcpClientBootstrap bootstrap ) {
    	this.bootstrap = bootstrap;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = pipeline();
        
        RpcSSLContext ssl = bootstrap.getSslContext();
        if ( ssl != null ) {
        	p.addLast(Handler.SSL, new SslHandler(ssl.createClientEngine()) );
        }

        p.addLast(Handler.FRAME_DECODER, new ProtobufVarint32FrameDecoder());
        p.addLast(Handler.PROTOBUF_DECODER, new ProtobufDecoder(DuplexProtocol.WirePayload.getDefaultInstance(),bootstrap.getExtensionRegistry()));

        p.addLast(Handler.FRAME_ENCODER, new ProtobufVarint32LengthFieldPrepender());
        p.addLast(Handler.PROTOBUF_ENCODER, new ProtobufEncoder());

        // the connectResponseHandler is swapped after the client connection
        // handshake with the RpcClient for the Channel
        p.addLast(Handler.CLIENT_CONNECT, new ClientConnectResponseHandler());

        return p;
    }

}
