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
package com.googlecode.protobuf.pro.stream.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

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

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.stream.PeerInfo;
import com.googlecode.protobuf.pro.stream.RpcSSLContext;
import com.googlecode.protobuf.pro.stream.StreamingClient;
import com.googlecode.protobuf.pro.stream.TransferIn;
import com.googlecode.protobuf.pro.stream.TransferOut;
import com.googlecode.protobuf.pro.stream.handler.Handler;
import com.googlecode.protobuf.pro.stream.handler.StreamingClientHandler;
import com.googlecode.protobuf.pro.stream.logging.CategoryPerMessageTypeLogger;
import com.googlecode.protobuf.pro.stream.logging.StreamLogger;

public class StreamingTcpClientBootstrap<E extends Message, F extends Message> extends ClientBootstrap {

	private static Log log = LogFactory.getLog(StreamingTcpClientBootstrap.class);
	
	private final PeerInfo clientInfo;
	private StreamLogger streamLogger = new CategoryPerMessageTypeLogger();
	private boolean shareChannels = true;
	private int chunkSize = 1400 * 64; // default is 64 basic IP packet ( MTU=1500 - chunk overhead )

	/**
	 * All Netty Channels created and bound by this StreamingTcpClientBootstrap.
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
    public StreamingTcpClientBootstrap(PeerInfo clientInfo, ChannelFactory channelFactory ) {
        super(channelFactory);
        this.clientInfo = clientInfo;
        setPipelineFactory(new StreamingTcpClientPipelineFactory());
    }
    
	public TransferIn pull( PeerInfo serverInfo, E message ) throws IOException {
        // Make a new connection.
		InetSocketAddress remoteAddress = new InetSocketAddress(serverInfo.getHostName(), serverInfo.getPort());
		
		StreamingClient<E,F> streamingClient = findExistingChannelTo(remoteAddress);
		if ( streamingClient == null || !shareChannels ) {
			streamingClient = connectWith(remoteAddress);
		} else {
			log.debug("Reusing open connection to " + serverInfo + " for pull.");
		}
		
		return streamingClient.pull(message);
	}

	public TransferOut push( PeerInfo serverInfo, F message ) throws IOException {
        // Make a new connection.
		InetSocketAddress remoteAddress = new InetSocketAddress(serverInfo.getHostName(), serverInfo.getPort());
		
		StreamingClient<E,F> streamingClient = findExistingChannelTo(remoteAddress);
		if ( streamingClient == null || !shareChannels ) {
			streamingClient = connectWith(remoteAddress);
		} else {
			log.debug("Reusing open connection to " + serverInfo + " for push." );
		}
		
		return streamingClient.push(message);
	}

	@SuppressWarnings("unchecked")
	StreamingClient<E,F> findExistingChannelTo( InetSocketAddress address ) {
		// traverse all channels and try and find one which matches the address
		// and then find the streaming client in it's pipeline
		for( Channel c : allChannels ) {
			InetSocketAddress remoteAddress = (InetSocketAddress)c.getRemoteAddress();
			if ( remoteAddress.equals(address)) {
				StreamingClientHandler<E,F> streamingClientHandler = (StreamingClientHandler<E,F>)c.getPipeline().get(Handler.STREAMING_CLIENT);
				if ( streamingClientHandler != null ) {
					return streamingClientHandler.getStreamingClient();
				}
			}
		}
		return null;
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
    private StreamingClient<E,F> connectWith(InetSocketAddress remoteAddress) throws IOException {
        if (remoteAddress == null) {
            throw new NullPointerException("remotedAddress");
        }
        SocketAddress localAddress = (SocketAddress) getOption("localAddress");
        ChannelFuture connectFuture = super.connect(remoteAddress,localAddress).awaitUninterruptibly();
        
        if ( !connectFuture.isSuccess() ) {
    		throw new IOException("Failed to connect to " + remoteAddress, connectFuture.getCause());
        }
        
        Channel channel = connectFuture.getChannel();
		
		PeerInfo serverInfo = new PeerInfo(remoteAddress.getHostName(), remoteAddress.getPort(), "<N/A>" );
		
		StreamingClient<E,F> streamingClient = new StreamingClient<E,F>(channel, clientInfo, serverInfo, chunkSize);
		streamingClient.setStreamLogger(getStreamLogger());
		
		completePipeline(streamingClient);

		allChannels.add(channel);
		// channels remove themselves when closed.
        return streamingClient;
    }
    
	/**
	 * After connection has taken place, we modify the pipeline.
	 * 
	 * @param streamingClient
	 * @return
	 */
    protected StreamingClientHandler<E,F> completePipeline(StreamingClient<E,F> streamingClient) {
		StreamingClientHandler<E,F> streamingClientHandler = new StreamingClientHandler<E,F>(streamingClient);
		streamingClient.getChannel().getPipeline().addLast(Handler.STREAMING_CLIENT, streamingClientHandler);
		
		return streamingClientHandler;
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
	}

	@Override
	public String toString() {
		return "ClientBootstrap:"+clientInfo;
	}
	
	@Override
	public ChannelFuture connect(SocketAddress remoteAddress) {
		throw new IllegalStateException("use push or pull method.");
	}
	@Override
	public ChannelFuture connect(final SocketAddress remoteAddress, final SocketAddress localAddress) {
		throw new IllegalStateException("use push or pull method.");
	}
	@Override
	public ChannelFuture connect() {
		throw new IllegalStateException("use push or pull method.");
	}
	
	/**
	 * @return the clientInfo
	 */
	public PeerInfo getClientInfo() {
		return clientInfo;
	}

	/**
	 * @return the sslContext
	 */
	public RpcSSLContext getSslContext() {
		return ((StreamingTcpClientPipelineFactory)getPipelineFactory()).getSslContext();
	}

	/**
	 * @param sslContext the sslContext to set
	 */
	public void setSslContext(RpcSSLContext sslContext) {
		((StreamingTcpClientPipelineFactory)getPipelineFactory()).setSslContext(sslContext);
	}

	/**
	 * @return the streamLogger
	 */
	public StreamLogger getStreamLogger() {
		return streamLogger;
	}

	/**
	 * @param streamLogger the streamLogger to set
	 */
	public void setStreamLogger(StreamLogger streamLogger) {
		this.streamLogger = streamLogger;
	}

	/**
	 * @return the chunkSize
	 */
	public int getChunkSize() {
		return chunkSize;
	}

	/**
	 * @param chunkSize the chunkSize to set
	 */
	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	/**
	 * @return the shareChannels
	 */
	public boolean isShareChannels() {
		return shareChannels;
	}

	/**
	 * @param shareChannels the shareChannels to set
	 */
	public void setShareChannels(boolean shareChannels) {
		this.shareChannels = shareChannels;
	}

	/**
	 * @return the compress
	 */
	public boolean isCompress() {
		return ((StreamingTcpClientPipelineFactory)getPipelineFactory()).isCompress();
	}

	/**
	 * @param compress the compress to set
	 */
	public void setCompress(boolean compress) {
		((StreamingTcpClientPipelineFactory)getPipelineFactory()).setCompress(compress);
	}
}
