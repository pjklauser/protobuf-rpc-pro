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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

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
public class RpcServerHandler extends SimpleChannelUpstreamHandler {

	private static Log log = LogFactory.getLog(RpcServerHandler.class);

    private RpcServer rpcServer;
    private RpcClientRegistry rpcClientRegistry; // only used on server side
    
    public RpcServerHandler(RpcServer rpcServer) {
    	if ( rpcServer == null ) {
    		throw new IllegalArgumentException("rpcServer");
    	}
    	this.rpcServer = rpcServer;
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if ( e.getMessage() instanceof WirePayload) {
        	WirePayload payload = (WirePayload)e.getMessage();
        	if ( payload.hasRpcRequest() ) {
        		rpcServer.request(payload.getRpcRequest());
        		return;
        	} else if ( payload.hasRpcCancel() ) {
        		rpcServer.cancel(payload.getRpcCancel());
        		return;
        	}
        	// serverMessage, unsolicitedMessage, rpcResponse, rpcError were consumed further down by RpcClientHandler.
        	// everything else is passed through to potentially later channel handlers which are modified by using code.
        }
        ctx.sendUpstream(e);
    }

    @Override
    public void channelClosed(
            ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	ctx.sendUpstream(e);
    	if ( rpcClientRegistry != null ) {
    		// we could have used a separate channel pipeline handler to 
    		// disconnect clients from the client registry just on the server
    		// side, then the RpcServerHandler would be symmetric on client and server sides.
    		// but i decided against it for (probably negligible) performance
    		// reasons - lengthening the chain lessens performance 
    		rpcClientRegistry.removeRpcClient(rpcServer.getRcpClient());
    	}
    	rpcServer.handleClosure();
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    	log.warn("Exception caught during RPC operation.", e.getCause());
    	ctx.getChannel().close();
    	rpcServer.getRcpClient().handleClosure();
    }
    
	/**
	 * @return the rpcClientRegistry
	 */
	public RpcClientRegistry getRpcClientRegistry() {
		return rpcClientRegistry;
	}

	/**
	 * @param rpcClientRegistry the rpcClientRegistry to set
	 */
	public void setRpcClientRegistry(RpcClientRegistry rpcClientRegistry) {
		this.rpcClientRegistry = rpcClientRegistry;
	}

	/**
	 * @return the rpcServer
	 */
	public RpcServer getRpcServer() {
		return rpcServer;
	}

}
