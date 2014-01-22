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
package com.googlecode.protobuf.pro.duplex.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ExtensionRegistry;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcSSLContext;
import com.googlecode.protobuf.pro.duplex.RpcServer;
import com.googlecode.protobuf.pro.duplex.RpcServiceRegistry;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.handler.ClientConnectResponseHandler;
import com.googlecode.protobuf.pro.duplex.handler.Handler;
import com.googlecode.protobuf.pro.duplex.handler.RpcClientHandler;
import com.googlecode.protobuf.pro.duplex.handler.RpcServerHandler;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogger;
import com.googlecode.protobuf.pro.duplex.server.RpcClientRegistry;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.ConnectRequest;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.ConnectResponse;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.WirePayload;

public class DuplexTcpClientPipelineFactory extends ChannelInitializer<Channel> {

	private static Logger log = LoggerFactory.getLogger(DuplexTcpClientPipelineFactory.class);
	
	private List<TcpConnectionEventListener> connectionEventListeners = new ArrayList<TcpConnectionEventListener>();

	/**
	 * Setting the clientInfo to a PeerInfo which has a hostname and port will bind the local TCP endpoint
	 * to a specific port.
	 * 
	 * This will have the effect that only a single outgoing connection can be made to a RPC server.
	 * This is useful only if the server is statefull and interested when the "same" client (re)connects. 
	 * 
	 * see Issue 30: https://code.google.com/p/protobuf-rpc-pro/issues/detail?id=30
	 */
	private PeerInfo clientInfo;
	
	/**
	 * Whether socket level communications between ALL clients peered with servers by this
	 * Bootstrap should be compressed ( using ZLIB ).
	 */
	private boolean compression = false;
	
	private AtomicInteger correlationId = new AtomicInteger(1);

	private final RpcClientRegistry rpcClientRegistry = new RpcClientRegistry();
	private final RpcServiceRegistry rpcServiceRegistry = new RpcServiceRegistry();
	private RpcServerCallExecutor rpcServerCallExecutor = null;
	private ExtensionRegistry extensionRegistry;
	private ExtensionRegistry wirelinePayloadExtensionRegistry;
	private RpcSSLContext sslContext;
	private RpcLogger logger = new CategoryPerServiceLogger();
	private long connectResponseTimeoutMillis = ClientConnectResponseHandler.DEFAULT_CONNECT_RESPONSE_TIMEOUT_MS;
	
	/**
	 * Create a factory for RpcClients.
	 * 
	 * To force use of a specific local port binding use {@link #setClientInfo(PeerInfo)}.
	 * 
	 * @param clientInfo
	 */
    public DuplexTcpClientPipelineFactory() {
    	this.clientInfo = new PeerInfo();
    }

	public RpcClient peerWith( PeerInfo serverInfo, Bootstrap bootstrap ) throws IOException {
        // Make a new connection.
		InetSocketAddress remoteAddress = new InetSocketAddress(serverInfo.getHostName(), serverInfo.getPort());
		return peerWith(remoteAddress, bootstrap);
	}

	public RpcClient peerWith( String host, int port, Bootstrap bootstrap ) throws IOException {
        // Make a new connection.
		InetSocketAddress remoteAddress = new InetSocketAddress(host, port);
		return peerWith(remoteAddress, bootstrap);
	}

    /**
     * Attempts a new connection with the specified {@code remoteAddress}.
     *
     * @return a future object which notifies when this connection attempt
     *         succeeds or fails
     *
     * @throws IOException
     *         if the peering failed.
     */
    public RpcClient peerWith(InetSocketAddress remoteAddress, Bootstrap bootstrap ) throws IOException {
        if (remoteAddress == null) {
            throw new NullPointerException("remotedAddress");
        }
        InetSocketAddress localAddress = null;
        if ( clientInfo.getHostName() != null ) {
        	localAddress = new InetSocketAddress(clientInfo.getHostName(), clientInfo.getPort());
        }
        ChannelFuture connectFuture = bootstrap.connect(remoteAddress, localAddress).awaitUninterruptibly();
        
        if ( !connectFuture.isSuccess() ) {
    		throw new IOException("Failed to connect to " + remoteAddress, connectFuture.cause());
        }
        
        Channel channel = connectFuture.channel();
        InetSocketAddress connectedAddress = (InetSocketAddress) channel.localAddress();
        
        PeerInfo effectiveClientInfo = new PeerInfo( clientInfo.getHostName() == null ? connectedAddress.getHostName() : clientInfo.getHostName(),
        		connectedAddress.getPort(), clientInfo.getPid() );
        
		ConnectRequest connectRequest = ConnectRequest.newBuilder()
		.setClientHostName(effectiveClientInfo.getHostName())
		.setClientPort(effectiveClientInfo.getPort())
		.setClientPID(effectiveClientInfo.getPid())
		.setCorrelationId(correlationId.incrementAndGet())
		.setCompress(isCompression())
		.build();
        
		WirePayload payload = WirePayload.newBuilder().setConnectRequest(connectRequest).build();
		if ( log.isDebugEnabled() ) {
			log.debug("Sending ["+connectRequest.getCorrelationId()+"]ConnectRequest.");
		}
		channel.writeAndFlush(payload);
		
		ClientConnectResponseHandler connectResponseHandler = (ClientConnectResponseHandler)channel.pipeline().get(Handler.CLIENT_CONNECT);
		if ( connectResponseHandler == null ) {
			throw new IllegalStateException("No connectReponse handler in channel pipeline.");
		}
        
		ConnectResponse connectResponse = connectResponseHandler.getConnectResponse(connectResponseTimeoutMillis);
		if ( connectResponse == null ) {
			connectFuture.channel().close().awaitUninterruptibly();
			throw new IOException("No Channel response received before " + connectResponseTimeoutMillis + " millis timeout.");
		}
		if ( connectResponse.hasErrorCode() ) {
        	connectFuture.channel().close().awaitUninterruptibly();
			throw new IOException("DuplexTcpServer CONNECT_RESPONSE indicated error " + connectResponse.getErrorCode());
		}
		if ( !connectResponse.hasCorrelationId() ) {
        	connectFuture.channel().close().awaitUninterruptibly();
			throw new IOException("DuplexTcpServer CONNECT_RESPONSE missing correlationId.");
		}
		if ( connectResponse.getCorrelationId() != connectRequest.getCorrelationId() ) {
        	connectFuture.channel().close().awaitUninterruptibly();
			throw new IOException("DuplexTcpServer CONNECT_RESPONSE correlationId mismatch. TcpClient sent " + connectRequest.getCorrelationId() + " received " + connectResponse.getCorrelationId() + " from TcpServer.");
		}
		PeerInfo serverInfo = null;
		if ( connectResponse.hasServerPID() ) {
			serverInfo = new PeerInfo(remoteAddress.getHostName(), remoteAddress.getPort(), connectResponse.getServerPID() );
		} else {
			serverInfo = new PeerInfo(remoteAddress.getHostName(), remoteAddress.getPort() );
		}
		
		RpcClient rpcClient = new RpcClient(channel, effectiveClientInfo, serverInfo, connectResponse.getCompress(), getRpcLogger(), getExtensionRegistry());
		
		RpcClientHandler rpcClientHandler = completePipeline(rpcClient);
		rpcClientHandler.notifyOpened();
		
		// register the rpcClient in the RpcClientRegistry
		if ( !getRpcClientRegistry().registerRpcClient(rpcClient) ) {
			log.warn("Client RpcClient already registered. Bug??");
		}
		// channels remove themselves when closed.
        return rpcClient;
    }
    
	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelInitializer#initChannel(io.netty.channel.Channel)
	 */
	@Override
	protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        
        RpcSSLContext ssl = getSslContext();
        if ( ssl != null ) {
        	p.addLast(Handler.SSL, new SslHandler(ssl.createClientEngine()) );
        }

        p.addLast(Handler.FRAME_DECODER, new ProtobufVarint32FrameDecoder());
        p.addLast(Handler.PROTOBUF_DECODER, new ProtobufDecoder(DuplexProtocol.WirePayload.getDefaultInstance(),getWirelinePayloadExtensionRegistry()));

        p.addLast(Handler.FRAME_ENCODER, new ProtobufVarint32LengthFieldPrepender());
        p.addLast(Handler.PROTOBUF_ENCODER, new ProtobufEncoder());

        // the connectResponseHandler is swapped after the client connection
        // handshake with the RpcClient for the Channel
        p.addLast(Handler.CLIENT_CONNECT, new ClientConnectResponseHandler());
	}


	/**
	 * After RPC handshake has taken place, remove the RPC handshake
	 * {@link ClientConnectResponseHandler} and add a {@link RpcClientHandler}
	 * and {@link RpcServerHandler} to complete the Netty client side Pipeline.
	 * 
	 * @param rpcClient
	 * @return
	 */
    protected RpcClientHandler completePipeline(RpcClient rpcClient) {
		ChannelPipeline p = rpcClient.getChannel().pipeline();
		
		if ( rpcClient.isCompression() ) {
	    	p.addBefore(Handler.FRAME_DECODER, Handler.COMPRESSOR, ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
	    	p.addAfter(Handler.COMPRESSOR, Handler.DECOMPRESSOR, ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
    	}
    	
		TcpConnectionEventListener informer = new TcpConnectionEventListener(){
			@Override
			public void connectionClosed(RpcClientChannel client) {
				for( TcpConnectionEventListener listener : getListenersCopy() ) {
					listener.connectionClosed(client);
				}
			}
			@Override
			public void connectionOpened(RpcClientChannel client) {
				for( TcpConnectionEventListener listener : getListenersCopy() ) {
					listener.connectionOpened(client);
				}
			}
		};
		RpcClientHandler rpcClientHandler = new RpcClientHandler(rpcClient, informer);
		p.replace(Handler.CLIENT_CONNECT, Handler.RPC_CLIENT, rpcClientHandler);
		
		RpcServer rpcServer = new RpcServer(rpcClient, rpcServiceRegistry, rpcServerCallExecutor, logger);
		RpcServerHandler rpcServerHandler = new RpcServerHandler(rpcServer,rpcClientRegistry); 
		p.addAfter(Handler.RPC_CLIENT, Handler.RPC_SERVER, rpcServerHandler);
		
		return rpcClientHandler;
    }
    
	@Override
	public String toString() {
		return "DuplexTcpClientPipelineFactory:"+clientInfo;
	}
	
	public void registerConnectionEventListener( TcpConnectionEventListener listener ) {
		getConnectionEventListeners().add(listener);
	}
	
	public void removeConnectionEventListener( TcpConnectionEventListener listener ) {
		getConnectionEventListeners().remove(listener);
	}
	
	private List<TcpConnectionEventListener> getListenersCopy() {
		List<TcpConnectionEventListener> copy = new ArrayList<TcpConnectionEventListener>();
		copy.addAll(getConnectionEventListeners());
		
		return Collections.unmodifiableList(copy);
	}
	
	/**
	 * @return the connectionEventListeners
	 */
	public List<TcpConnectionEventListener> getConnectionEventListeners() {
		if ( connectionEventListeners == null ) {
			return new ArrayList<TcpConnectionEventListener>(0);
		}
		return connectionEventListeners;
	}

	/**
	 * @param connectionEventListeners the connectionEventListeners to set
	 */
	public void setConnectionEventListeners(
			List<TcpConnectionEventListener> connectionEventListeners) {
		this.connectionEventListeners = connectionEventListeners;
	}

	/**
	 * @return the clientInfo
	 */
	public PeerInfo getClientInfo() {
		return clientInfo;
	}

	/**
	 * There can be only one peering with a single server if this method is used to
	 * set a specific client peerInfo.
	 * 
	 * If you want to use a free client port each time, then use the default constructor and
	 * never set this clientInfo.
	 * 
	 * @param clientInfo the clientInfo to set to force a local port binding.
	 */
	public void setClientInfo(PeerInfo clientInfo) {
		this.clientInfo = clientInfo;
	}

	/**
	 * @return the rpcServiceRegistry
	 */
	public RpcServiceRegistry getRpcServiceRegistry() {
		return rpcServiceRegistry;
	}

	/**
	 * @return the rpcClientRegistry
	 */
	public RpcClientRegistry getRpcClientRegistry() {
		return rpcClientRegistry;
	}

	/**
	 * @return the rpcServerCallExecutor
	 */
	public RpcServerCallExecutor getRpcServerCallExecutor() {
		return rpcServerCallExecutor;
	}

	/**
	 * @param rpcCallExecutor the rpcCallExecutor to set
	 */
	public void setRpcServerCallExecutor(RpcServerCallExecutor rpcServerCallExecutor) {
		this.rpcServerCallExecutor = rpcServerCallExecutor;
	}

	/**
	 * @return the rpcLogger
	 */
	public RpcLogger getRpcLogger() {
		return logger;
	}

	/**
	 * @param rpcLogger the rpcLogger to set
	 */
	public void setRpcLogger(RpcLogger rpcLogger) {
		this.logger = rpcLogger;
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
	 * @return the compression
	 */
	public boolean isCompression() {
		return compression;
	}

	/**
	 * @param compression the compression to set
	 */
	public void setCompression(boolean compression) {
		this.compression = compression;
	}

	/**
	 * @return the registered WirelinePayload's extension registry.
	 */
	public ExtensionRegistry getWirelinePayloadExtensionRegistry() {
		return wirelinePayloadExtensionRegistry;
	}
	
	/**
	 * Set the WirelinePayload's extension registry.
	 * 
	 * @param extensionRegistry
	 */
	public void setWirelinePayloadExtensionRegistry( ExtensionRegistry wirelinePayloadExtensionRegistry ) {
		this.wirelinePayloadExtensionRegistry = wirelinePayloadExtensionRegistry;
	}

	/**
	 * @return the extensionRegistry
	 */
	public ExtensionRegistry getExtensionRegistry() {
		return extensionRegistry;
	}

	/**
	 * @param extensionRegistry the extensionRegistry to set
	 */
	public void setExtensionRegistry(ExtensionRegistry extensionRegistry) {
		this.extensionRegistry = extensionRegistry;
	}

	/**
	 * @return the connectResponseTimeoutMillis
	 */
	public long getConnectResponseTimeoutMillis() {
		return connectResponseTimeoutMillis;
	}

	/**
	 * @param connectResponseTimeoutMillis the connectResponseTimeoutMillis to set
	 */
	public void setConnectResponseTimeoutMillis(long connectResponseTimeoutMillis) {
		this.connectResponseTimeoutMillis = connectResponseTimeoutMillis;
	}
}
