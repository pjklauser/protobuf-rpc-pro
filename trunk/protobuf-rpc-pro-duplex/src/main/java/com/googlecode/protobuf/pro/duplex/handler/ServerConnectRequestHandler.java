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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerPipelineFactory;
import com.googlecode.protobuf.pro.duplex.server.RpcClientRegistry;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.ConnectErrorCode;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.ConnectRequest;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.ConnectResponse;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.WirePayload;

/**
 * The ServerConnectRequestHandler handles the receipt of ConnectRequest
 * client requests, and uses the {@link RpcClientRegistry} to try to
 * register new clients. If the RpcClientRegistry allows the connection,
 * this handler sends back a ConnectResponse to the client.
 * 
 * Once a successful client handshake has been performed, this Handler
 * uses the {@link DuplexTcpServerPipelineFactory} to complete the
 * Channel's pipeline, which will remove this Handler ( since it's job is
 * done ) and place a {@link RpcClientHandler} and {@link RpcServerHandler}
 * into the pipeline.
 * 
 * @author Peter Klauser
 *
 */
@Sharable
public class ServerConnectRequestHandler extends MessageToMessageDecoder<WirePayload> {

	private static Logger log = LoggerFactory.getLogger(ServerConnectRequestHandler.class);

    private final DuplexTcpServerPipelineFactory pipelineFactory;
    
    public ServerConnectRequestHandler( DuplexTcpServerPipelineFactory pipelineFactory ) {
    	this.pipelineFactory = pipelineFactory;
    }
    
	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelInboundMessageHandlerAdapter#messageReceived(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	protected void decode(ChannelHandlerContext ctx, WirePayload msg,
			List<Object> out) throws Exception {
		if ( msg.hasConnectRequest() ) {
			ConnectRequest connectRequest = msg.getConnectRequest();
    		if ( log.isDebugEnabled() ) {
    			log.debug("Received ["+connectRequest.getCorrelationId()+"]ConnectRequest.");
    		}
    		PeerInfo connectingClientInfo = new PeerInfo(connectRequest.getClientHostName(), connectRequest.getClientPort(), connectRequest.getClientPID());
    		ConnectResponse connectResponse = null;
    		
    		RpcClient rpcClient = new RpcClient(ctx.channel(), pipelineFactory.getServerInfo(), connectingClientInfo, connectRequest.getCompress(), pipelineFactory.getLogger(), pipelineFactory.getExtensionRegistry() );
    		if ( pipelineFactory.getRpcClientRegistry().registerRpcClient(rpcClient) ) {
    			connectResponse = ConnectResponse.newBuilder().setCorrelationId(connectRequest.getCorrelationId())
    					.setServerPID(pipelineFactory.getServerInfo().getPid())
    					.setCompress(connectRequest.getCompress())
    					.build();
        		WirePayload payload = WirePayload.newBuilder().setConnectResponse(connectResponse).build();
        		
        		if ( log.isDebugEnabled() ) {
        			log.debug("Sending ["+connectResponse.getCorrelationId()+"]ConnectResponse.");
        		}
        		ctx.channel().writeAndFlush(payload);
        		
        		// now we swap this Handler out of the pipeline and complete the server side pipeline.
        		RpcClientHandler clientHandler = pipelineFactory.completePipeline(rpcClient);
        		clientHandler.notifyOpened();
    		} else {
    			connectResponse = ConnectResponse.newBuilder().setCorrelationId(connectRequest.getCorrelationId()).setErrorCode(ConnectErrorCode.ALREADY_CONNECTED).build();
        		WirePayload payload = WirePayload.newBuilder().setConnectResponse(connectResponse).build();
        		
        		if ( log.isDebugEnabled() ) {
        			log.debug("Sending ["+connectResponse.getCorrelationId()+"]ConnectResponse. Already Connected.");
        		}
        		ChannelFuture future = ctx.channel().writeAndFlush(payload);
        		future.addListener(ChannelFutureListener.CLOSE); // close after write response.
    		}
    	} else {
    		out.add(msg);
    	}
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
