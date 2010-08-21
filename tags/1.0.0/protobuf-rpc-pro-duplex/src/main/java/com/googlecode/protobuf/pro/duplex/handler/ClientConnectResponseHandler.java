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
package com.googlecode.protobuf.pro.duplex.handler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.ConnectResponse;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.WirePayload;

public class ClientConnectResponseHandler extends SimpleChannelUpstreamHandler {

	private static Log log = LogFactory.getLog(ClientConnectResponseHandler.class);

    public static final long DEFAULT_CONNECT_RESPONSE_TIMEOUT_MS = 10000;
    
    private final BlockingQueue<ConnectResponse> answerQueue = new LinkedBlockingQueue<ConnectResponse>();

    private final ConnectResponse EMPTY_RESPONSE = ConnectResponse.newBuilder().setCorrelationId(0).build();
    
    public ConnectResponse getConnectResponse(long timeoutMillis) {
        try {
        	ConnectResponse connectResponse = answerQueue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            return connectResponse != EMPTY_RESPONSE ? connectResponse : null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if ( e.getMessage() instanceof WirePayload) {
        	ConnectResponse connectResponse = ((WirePayload)e.getMessage()).getConnectResponse();
        	if ( connectResponse != null ) {
        		if ( log.isDebugEnabled() ) {
        			log.debug("Received ["+connectResponse.getCorrelationId()+"]ConnectResponse.");
        		}
        		answerQueue.put(connectResponse);
        		return;
        	}
        }
        ctx.sendUpstream(e);
    }

    @Override
    public void channelClosed(
            ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	answerQueue.put(EMPTY_RESPONSE);
        ctx.sendUpstream(e);
    }
    
    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    	log.warn("Exception caught during RPC connection handshake.", e.getCause());
    	if ( ctx.getChannel().isConnected() ) {
    		ctx.getChannel().close();
    	}
    }
}
