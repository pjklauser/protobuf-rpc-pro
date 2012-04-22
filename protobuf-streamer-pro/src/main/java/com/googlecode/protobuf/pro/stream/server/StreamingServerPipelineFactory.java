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

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.stream.PeerInfo;
import com.googlecode.protobuf.pro.stream.RpcSSLContext;
import com.googlecode.protobuf.pro.stream.StreamingServer;
import com.googlecode.protobuf.pro.stream.handler.Handler;
import com.googlecode.protobuf.pro.stream.handler.StreamingServerHandler;
import com.googlecode.protobuf.pro.stream.logging.StreamLogger;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol;

public class StreamingServerPipelineFactory<E extends Message, F extends Message> implements
        ChannelPipelineFactory {

	private final PeerInfo serverInfo;
	private final StreamLogger logger;
	private final PullHandler<E> pullHandler;
	private final PushHandler<F> pushHandler;
	
	private RpcSSLContext sslContext;
	private boolean compress;
	private int chunkSize = 64 * 1400; // 64 IP-packets IP MTU-overhead
	
	
	public StreamingServerPipelineFactory( PeerInfo serverInfo, PullHandler<E> pullHandler, PushHandler<F> pushHandler, StreamLogger logger ) {
		if ( serverInfo == null ) {
			throw new IllegalArgumentException("serverInfo");
		}
		this.serverInfo = serverInfo;
		this.pullHandler = pullHandler;
		this.pushHandler = pushHandler;
		this.logger = logger;
	}
	
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = pipeline();

        if ( getSslContext() != null ) {
        	p.addLast(Handler.SSL, new SslHandler(getSslContext().createServerEngine()) );
        }
        
        // clients configure compression if the server has it configured.
        if ( isCompress() ) {
	    	p.addLast( Handler.DECOMPRESSOR, new ZlibEncoder(ZlibWrapper.GZIP));
	    	p.addLast( Handler.COMPRESSOR,  new ZlibDecoder(ZlibWrapper.GZIP));
        }
        
        p.addLast(Handler.FRAME_DECODER, new ProtobufVarint32FrameDecoder());
        p.addLast(Handler.PROTOBUF_DECODER, new ProtobufDecoder(StreamProtocol.WirePayload.getDefaultInstance()));

        p.addLast(Handler.FRAME_ENCODER, new ProtobufVarint32LengthFieldPrepender());
        p.addLast(Handler.PROTOBUF_ENCODER, new ProtobufEncoder());

        StreamingServer<E,F> streamingServer = new StreamingServer<E, F>(serverInfo, pullHandler, pushHandler, logger, chunkSize);
        p.addLast(Handler.STREAMING_SERVER, new StreamingServerHandler<E, F>(streamingServer));
        return p;
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
	 * @return the chunkSize
	 */
	public int getChunkSize() {
		return chunkSize;
	}

	/**
	 * @param chunkSize the chunkSize to set
	 */
	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	/**
	 * @return the compress
	 */
	public boolean isCompress() {
		return compress;
	}

	/**
	 * @param compress the compress to set
	 */
	public void setCompress(boolean compress) {
		this.compress = compress;
	}
}
