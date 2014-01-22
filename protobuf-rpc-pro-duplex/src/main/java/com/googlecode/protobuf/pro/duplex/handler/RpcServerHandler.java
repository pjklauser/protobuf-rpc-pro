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

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.protobuf.pro.duplex.RpcServer;
import com.googlecode.protobuf.pro.duplex.server.RpcClientRegistry;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.WirePayload;

/**
 * A pipeline handler which handles incoming RpcRequest and
 * RpcCancel payloads towards a {@link RpcServer}.
 * 
 * @author Peter Klauser
 *
 */
public class RpcServerHandler extends MessageToMessageDecoder<WirePayload> {

	private static Logger log = LoggerFactory.getLogger(RpcServerHandler.class);

    private final RpcServer rpcServer;
    private final RpcClientRegistry rpcClientRegistry;
    
    public RpcServerHandler(RpcServer rpcServer, RpcClientRegistry rpcClientRegistry) {
    	if ( rpcServer == null ) {
    		throw new IllegalArgumentException("rpcServer");
    	}
    	if ( rpcClientRegistry == null ) {
    		throw new IllegalArgumentException("rpcClientRegistry");
    	}
    	this.rpcServer = rpcServer;
    	this.rpcClientRegistry = rpcClientRegistry;
    }

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelInboundMessageHandlerAdapter#decode(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	protected void decode(ChannelHandlerContext ctx, WirePayload msg,
			List<Object> out) throws Exception {
    	if ( msg.hasRpcRequest() ) {
    		rpcServer.request(msg.getRpcRequest());
    		return;
    	} else if ( msg.hasRpcCancel() ) {
    		rpcServer.cancel(msg.getRpcCancel());
    		return;
    	} else {
    	// serverMessage, unsolicitedMessage, rpcResponse, rpcError were consumed further down by RpcClientHandler.
    	// everything else is passed through to potentially later channel handlers which are modified by using code.
    		out.add(msg);
    	}
	}

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelStateHandlerAdapter#channelInactive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
   		rpcClientRegistry.removeRpcClient(rpcServer.getRcpClient());
    	rpcServer.handleClosure();
	}

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelHandlerAdapter#exceptionCaught(io.netty.channel.ChannelHandlerContext, java.lang.Throwable)
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
    	log.warn("Exception caught during RPC operation.", cause);
    	ctx.close();
    	rpcServer.getRcpClient().handleClosure();
    }
    
	/**
	 * @return the rpcClientRegistry
	 */
	public RpcClientRegistry getRpcClientRegistry() {
		return rpcClientRegistry;
	}

	/**
	 * @return the rpcServer
	 */
	public RpcServer getRpcServer() {
		return rpcServer;
	}


}
