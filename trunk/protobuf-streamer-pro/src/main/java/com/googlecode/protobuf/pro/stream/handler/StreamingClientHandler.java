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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.stream.StreamingClient;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.WirePayload;

/**
 * Handles incoming server responses which can be: 
 * 
 * Chunks being pulled back to Client
 * or CloseNotification from the server.
 * 
 * @author Peter Klauser
 *
 */
public class StreamingClientHandler<E extends Message,F extends Message> extends SimpleChannelUpstreamHandler {

    private final StreamingClient<E,F> streamingClient;
    
	private static Log log = LogFactory.getLog(StreamingClientHandler.class);

    public StreamingClientHandler(StreamingClient<E,F> streamingClient ) {
    	if ( streamingClient == null ) {
    		throw new IllegalArgumentException("streamingClient");
    	}
    	this.streamingClient = streamingClient;
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if ( e.getMessage() instanceof WirePayload) {
        	WirePayload payload = (WirePayload)e.getMessage();
        	if ( payload.hasChunk() ) {
        		streamingClient.pullChunk(payload.getChunk());
        		return;
        	} else if ( payload.hasClose() ) {
        		streamingClient.closeNotification(payload.getClose());
        		return;
        	}
        	// pushRequest, pullRequest should only be received on the server side.
        }
        ctx.sendUpstream(e);
    }

    @Override
    public void channelClosed(
            ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        ctx.sendUpstream(e);
        streamingClient.handleClosure();
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    	log.warn("Exception caught during Streaming operation.", e.getCause());
    	ctx.getChannel().close();
    	streamingClient.handleClosure();
    }

	/**
	 * @return the streamingClient
	 */
	public StreamingClient<E, F> getStreamingClient() {
		return streamingClient;
	}

}
