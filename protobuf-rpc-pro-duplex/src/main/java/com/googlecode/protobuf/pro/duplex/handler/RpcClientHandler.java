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

import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.WirePayload;

/**
 * Handles returning RpcResponse and RpcError messages
 * in the IO-Layer, delegating them to the Netty
 * Channel's {@link RpcClient}.
 * 
 * @author Peter Klauser
 *
 */
public class RpcClientHandler extends MessageToMessageDecoder<WirePayload> {

    private RpcClient rpcClient;
    private TcpConnectionEventListener eventListener;
    
    public RpcClientHandler(RpcClient rpcClient, TcpConnectionEventListener eventListener ) {
    	if ( rpcClient == null ) {
    		throw new IllegalArgumentException("rpcClient");
    	}
    	if ( eventListener == null ) {
    		throw new IllegalArgumentException("eventListener");
    	}
    	this.eventListener = eventListener;
    	this.rpcClient = rpcClient;
    }

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelInboundMessageHandlerAdapter#decode(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	protected void decode(ChannelHandlerContext ctx, WirePayload msg,
			List<Object> out) throws Exception {
    	if ( msg.hasRpcResponse() ) {
    		rpcClient.response(msg.getRpcResponse());
    		return;
    	} else if ( msg.hasRpcError() ) {
    		rpcClient.error(msg.getRpcError());
    		return;
    	} else if ( msg.hasOobResponse() ) {
    		rpcClient.receiveOobResponse(msg.getOobResponse());
    		return;
    	} else if ( msg.hasOobMessage() ) {
    		rpcClient.receiveOobMessage(msg.getOobMessage());
    		return;
    	} else if ( msg.hasTransparentMessage() ) {
    		// just so that it's not forgotten sometime...
    		out.add(msg);
    	} else {
        	// rpcRequest, rpcCancel, clientMessage go further up to the RpcServerHandler
        	// transparentMessage are also sent up but not handled anywhere explicitly 
    		out.add(msg);
    	}
    }

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelStateHandlerAdapter#channelInactive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
        rpcClient.handleClosure();
        notifyClosed();
	}

    public void notifyClosed() {
    	eventListener.connectionClosed(rpcClient);
    }

    public void notifyOpened() {
    	eventListener.connectionOpened(rpcClient);
    }
    
	/**
	 * @return the rpcClient
	 */
	public RpcClient getRpcClient() {
		return rpcClient;
	}

}
