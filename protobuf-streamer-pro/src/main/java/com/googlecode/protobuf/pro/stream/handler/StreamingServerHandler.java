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
package com.googlecode.protobuf.pro.stream.handler;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.stream.PeerInfo;
import com.googlecode.protobuf.pro.stream.StreamingServer;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.WirePayload;

/**
 * A pipeline handler which handles incoming Streaming
 * payloads on the server side and delegates towards a
 * {@link StreamingServer} specifically for the IO channel.
 * 
 * @author Peter Klauser
 *
 */
public class StreamingServerHandler<E extends Message, F extends Message> extends SimpleChannelUpstreamHandler {

	private static Log log = LogFactory.getLog(StreamingServerHandler.class);

    private StreamingServer<E,F> streamingServer;
    
    public StreamingServerHandler(StreamingServer<E,F> streamingServer) {
    	if ( streamingServer == null ) {
    		throw new IllegalArgumentException("streamingServer");
    	}
    	this.streamingServer = streamingServer;
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if ( e.getMessage() instanceof WirePayload) {
        	WirePayload payload = (WirePayload)e.getMessage();
        	if ( payload.hasPull() ) {
        		streamingServer.pullRequest(payload.getPull());
        	} else if ( payload.hasPush()) {
        		streamingServer.pushRequest(payload.getPush());
        	} else if ( payload.hasChunk()) {
        		streamingServer.pushChunk(payload.getChunk());
        	} else if ( payload.hasClose()) {
        		streamingServer.closeNotification(payload.getClose());
        	}
        	return;
        }
        ctx.sendUpstream(e);
    }

    @Override
    public void channelClosed(
            ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	ctx.sendUpstream(e);
    	streamingServer.handleClosure();
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    	log.warn("Exception caught during Streaming operation.", e.getCause());
    	ctx.getChannel().close();
    	streamingServer.handleClosure();
    }

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.SimpleChannelUpstreamHandler#channelOpen(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
	 */
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		super.channelOpen(ctx, e);
		
		InetSocketAddress address = (InetSocketAddress)ctx.getChannel().getRemoteAddress();
		PeerInfo clientInfo = new PeerInfo(address);
		streamingServer.handleOpen(clientInfo, ctx.getChannel());
	}
    
}
