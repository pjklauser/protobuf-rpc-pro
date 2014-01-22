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
package com.googlecode.protobuf.pro.duplex.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.ConnectResponse;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.WirePayload;

/**
 * The ClientConnectResponseHandler waits for a ConnectResponse
 * from the server's {@link ServerConnectRequestHandler} and
 * supplies this to the {@link DuplexTcpClientBootstrap} who
 * calls the {@link #getConnectResponse(long)}.
 * 
 * Once the server's ConnectResponse has been made, this handler
 * is removed from the Channel pipeline and replaced with the
 * {@link RpcClientHandler} and {@link RpcServerHandler}.
 * 
 * @author Peter Klauser
 *
 */
public class ClientConnectResponseHandler extends MessageToMessageDecoder<WirePayload> {

	private static Logger log = LoggerFactory.getLogger(ClientConnectResponseHandler.class);

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

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelInboundMessageHandlerAdapter#messageReceived(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	protected void decode(ChannelHandlerContext ctx, WirePayload msg,
			List<Object> out) throws Exception {
		if ( msg.hasConnectResponse() ) {
	    	ConnectResponse connectResponse = msg.getConnectResponse();
    		if ( log.isDebugEnabled() ) {
    			log.debug("Received ["+connectResponse.getCorrelationId()+"]ConnectResponse.");
    		}
    		answerQueue.put(connectResponse);
    		return;
		} else {
			out.add(msg);
		}
		
	}

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelStateHandlerAdapter#channelInactive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
    	answerQueue.put(EMPTY_RESPONSE);
    }
    
	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelHandlerAdapter#exceptionCaught(io.netty.channel.ChannelHandlerContext, java.lang.Throwable)
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		super.exceptionCaught(ctx, cause);
    	log.warn("Exception caught during RPC connection handshake.", cause);
    	ctx.close();
    }

}
