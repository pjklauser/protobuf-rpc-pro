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
package com.googlecode.protobuf.pro.duplex.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineException;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.codec.compression.ZlibWrapper;
import org.jboss.netty.util.internal.ConversionUtil;

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
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.ConnectRequest;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.ConnectResponse;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.WirePayload;

public class DuplexTcpClientBootstrap extends ClientBootstrap {

	private static Log log = LogFactory.getLog(DuplexTcpClientBootstrap.class);
	
	private List<TcpConnectionEventListener> connectionEventListeners = new ArrayList<TcpConnectionEventListener>();

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
	private RpcSSLContext sslContext;
	private RpcLogger logger = new CategoryPerServiceLogger();
	
	//TODO RuntimeConfigurator class
		// InlineReverseRpcCalling => cannot use SameThreadCallExecutor
			// ThreadPoolExecutor - unbounded/bounded queue, core, max threads, threadpoolfactory
		// Netty ChannelFactory - NioServerSocketChannelFactory(   Executors.newCachedThreadPool(), Executors.newCachedThreadPool())); 
		// TimeoutExecutor - thread factory, core poolsize, maxpoolsize,
		// TimeoutChecker - thread factory, corepoolsize
	
	/**
	 * All Netty Channels created and bound by this DuplexTcpClientBootstrap.
	 * 
	 * We keep hold of them to be able to do a clean shutdown.
	 */
	private ChannelGroup allChannels = new DefaultChannelGroup();
	
	/**
     * Creates a new instance stipulating client info.
     * 
     * @param clientInfo
     * @param channelFactory
     */
    public DuplexTcpClientBootstrap(PeerInfo clientInfo, ChannelFactory channelFactory ) {
        super(channelFactory);
        this.clientInfo = clientInfo;
        setPipelineFactory(new DuplexTcpClientPipelineFactory(this));
    }
    
	public RpcClient peerWith( PeerInfo serverInfo ) throws IOException {
        // Make a new connection.
		InetSocketAddress remoteAddress = new InetSocketAddress(serverInfo.getHostName(), serverInfo.getPort());
		return peerWith(remoteAddress);
	}

	public RpcClient peerWith( String host, int port ) throws IOException {
        // Make a new connection.
		InetSocketAddress remoteAddress = new InetSocketAddress(host, port);
		return peerWith(remoteAddress);
	}

    /**
     * Attempts a new connection with the specified {@code remoteAddress} and
     * the current {@code "localAddress"} option. If the {@code "localAddress"}
     * option is not set, the local address of a new channel is determined
     * automatically.  This method is identical with the following code:
     *
     * @return a future object which notifies when this connection attempt
     *         succeeds or fails
     *
     * @throws ClassCastException
     *         if {@code "localAddress"} option's value is
     *            neither a {@link SocketAddress} nor {@code null}
     * @throws ChannelPipelineException
     *         if this bootstrap's {@link #setPipelineFactory(ChannelPipelineFactory) pipelineFactory}
     *            failed to create a new {@link ChannelPipeline}
     */
    public RpcClient peerWith(InetSocketAddress remoteAddress) throws IOException {
        if (remoteAddress == null) {
            throw new NullPointerException("remotedAddress");
        }
        SocketAddress localAddress = (SocketAddress) getOption("localAddress");
        ChannelFuture connectFuture = super.connect(remoteAddress,localAddress).awaitUninterruptibly();
        
        if ( !connectFuture.isSuccess() ) {
    		throw new IOException("Failed to connect to " + remoteAddress, connectFuture.getCause());
        }
        
        Channel channel = connectFuture.getChannel();
        
		ConnectRequest connectRequest = ConnectRequest.newBuilder()
		.setClientHostName(clientInfo.getHostName())
		.setClientPort(clientInfo.getPort())
		.setClientPID(clientInfo.getPid())
		.setCorrelationId(correlationId.incrementAndGet())
		.setCompress(isCompression())
		.build();
        
		WirePayload payload = WirePayload.newBuilder().setConnectRequest(connectRequest).build();
		if ( log.isDebugEnabled() ) {
			log.debug("Sending ["+connectRequest.getCorrelationId()+"]ConnectRequest.");
		}
		channel.write(payload);
		
		ClientConnectResponseHandler connectResponseHandler = (ClientConnectResponseHandler)channel.getPipeline().get(Handler.CLIENT_CONNECT);
		if ( connectResponseHandler == null ) {
			throw new IllegalStateException("No connectReponse handler in channel pipeline.");
		}
        long connectResponseTimeoutMillis = ClientConnectResponseHandler.DEFAULT_CONNECT_RESPONSE_TIMEOUT_MS;
        if ( getOption("connectResponseTimeoutMillis") != null ) {
        	connectResponseTimeoutMillis = ConversionUtil.toInt(getOption("connectResponseTimeoutMillis"));
        }
        
		ConnectResponse connectResponse = connectResponseHandler.getConnectResponse(connectResponseTimeoutMillis);
		if ( connectResponse == null ) {
			connectFuture.getChannel().close().awaitUninterruptibly();
			throw new IOException("No Channel response received before " + connectResponseTimeoutMillis + " millis timeout.");
		}
		if ( connectResponse.hasErrorCode() ) {
        	connectFuture.getChannel().close().awaitUninterruptibly();
			throw new IOException("DuplexTcpServer CONNECT_RESPONSE indicated error " + connectResponse.getErrorCode());
		}
		if ( !connectResponse.hasCorrelationId() ) {
        	connectFuture.getChannel().close().awaitUninterruptibly();
			throw new IOException("DuplexTcpServer CONNECT_RESPONSE missing correlationId.");
		}
		if ( connectResponse.getCorrelationId() != connectRequest.getCorrelationId() ) {
        	connectFuture.getChannel().close().awaitUninterruptibly();
			throw new IOException("DuplexTcpServer CONNECT_RESPONSE correlationId mismatch. TcpClient sent " + connectRequest.getCorrelationId() + " received " + connectResponse.getCorrelationId() + " from TcpServer.");
		}
		PeerInfo serverInfo = null;
		if ( connectResponse.hasServerPID() ) {
			serverInfo = new PeerInfo(remoteAddress.getHostName(), remoteAddress.getPort(), connectResponse.getServerPID() );
		} else {
			serverInfo = new PeerInfo(remoteAddress.getHostName(), remoteAddress.getPort() );
		}
		
		RpcClient rpcClient = new RpcClient(channel, clientInfo, serverInfo, connectResponse.getCompress(), getRpcLogger());
		
		RpcClientHandler rpcClientHandler = completePipeline(rpcClient);
		rpcClientHandler.notifyOpened();
		
		allChannels.add(channel);
		// channels remove themselves when closed.
        return rpcClient;
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
		ChannelPipeline p = rpcClient.getChannel().getPipeline();
		
		if ( rpcClient.isCompression() ) {
	    	p.addBefore(Handler.FRAME_DECODER, Handler.DECOMPRESSOR, new ZlibEncoder(ZlibWrapper.GZIP));
	    	p.addAfter(Handler.DECOMPRESSOR, Handler.COMPRESSOR,  new ZlibDecoder(ZlibWrapper.GZIP));
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
    
	/* (non-Javadoc)
	 * @see org.jboss.netty.bootstrap.Bootstrap#releaseExternalResources()
	 */
	@Override
	public void releaseExternalResources() {
		log.debug("Closing all channels.");
		allChannels.close().awaitUninterruptibly();
		log.debug("Releasing IO-Layer external resources.");
		super.releaseExternalResources();
		if ( rpcServerCallExecutor != null ) {
			log.debug("Releasing RPC Executor external resources.");
			rpcServerCallExecutor.shutdown();
		}
	}

	@Override
	public String toString() {
		return "ClientBootstrap:"+clientInfo;
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
	
	@Override
	public ChannelFuture connect(SocketAddress remoteAddress) {
		throw new IllegalStateException("use peerWith method.");
	}
	@Override
	public ChannelFuture connect(final SocketAddress remoteAddress, final SocketAddress localAddress) {
		throw new IllegalStateException("use peerWith method.");
	}
	@Override
	public ChannelFuture connect() {
		throw new IllegalStateException("use peerWith method.");
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
	 * @param clientInfo the clientInfo to set
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

}
