/**
 *   Copyright 2010-2013 Peter Klauser
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;
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
public class ServerConnectRequestHandler extends SimpleChannelUpstreamHandler {

	private static Logger log = LoggerFactory.getLogger(ServerConnectRequestHandler.class);

	private final DuplexTcpServerBootstrap bootstrap;
    private final DuplexTcpServerPipelineFactory pipelineFactory;
    
    public ServerConnectRequestHandler( DuplexTcpServerBootstrap bootstrap, DuplexTcpServerPipelineFactory pipelineFactory ) {
    	this.bootstrap = bootstrap;
    	this.pipelineFactory = pipelineFactory;
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if ( e.getMessage() instanceof WirePayload) {
        	ConnectRequest connectRequest = ((WirePayload)e.getMessage()).getConnectRequest();
    		if ( log.isDebugEnabled() ) {
    			log.debug("Received ["+connectRequest.getCorrelationId()+"]ConnectRequest.");
    		}
        	if ( connectRequest != null ) {
        		PeerInfo connectingClientInfo = new PeerInfo(connectRequest.getClientHostName(), connectRequest.getClientPort(), connectRequest.getClientPID());
        		ConnectResponse connectResponse = null;
        		
        		RpcClient rpcClient = new RpcClient(ctx.getChannel(), bootstrap.getServerInfo(), connectingClientInfo, connectRequest.getCompress(), bootstrap.getLogger() );
        		if ( bootstrap.getRpcClientRegistry().registerRpcClient(rpcClient) ) {
        			connectResponse = ConnectResponse.newBuilder().setCorrelationId(connectRequest.getCorrelationId())
        					.setServerPID(bootstrap.getServerInfo().getPid())
        					.setCompress(connectRequest.getCompress())
        					.build();
            		WirePayload payload = WirePayload.newBuilder().setConnectResponse(connectResponse).build();
            		
            		if ( log.isDebugEnabled() ) {
            			log.debug("Sending ["+connectResponse.getCorrelationId()+"]ConnectResponse.");
            		}
            		ctx.getChannel().write(payload);
            		
            		// now we swap this Handler out of the pipeline and complete the server side pipeline.
            		RpcClientHandler clientHandler = pipelineFactory.completePipeline(rpcClient);
            		clientHandler.notifyOpened();
        		} else {
        			connectResponse = ConnectResponse.newBuilder().setCorrelationId(connectRequest.getCorrelationId()).setErrorCode(ConnectErrorCode.ALREADY_CONNECTED).build();
            		WirePayload payload = WirePayload.newBuilder().setConnectResponse(connectResponse).build();
            		
            		if ( log.isDebugEnabled() ) {
            			log.debug("Sending ["+connectResponse.getCorrelationId()+"]ConnectResponse. Already Connected.");
            		}
            		ChannelFuture future = ctx.getChannel().write(payload);
            		future.addListener(ChannelFutureListener.CLOSE); // close after write response.
        		}
        		return;
        	}
        }
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
