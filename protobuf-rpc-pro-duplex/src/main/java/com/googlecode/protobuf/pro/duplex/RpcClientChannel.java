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
package com.googlecode.protobuf.pro.duplex;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.listener.TcpConnectionEventListener;

/**
 * An RpcClientChannel is constructed once when a connection is established
 * with a server ( on both client and server ). The same RpcClientChannel
 * instance is used for the duration of the connection. 
 * 
 * If a client reconnects, a new RpcClientChannel instance is constructed. The 
 * {@link #getPeerInfo()} will also be a new instance, but the {@link com.googlecode.protobuf.pro.duplex.PeerInfo#getName()}
 * will be the same.
 * 
 * @author Peter Klauser
 *
 */
public interface RpcClientChannel extends com.google.protobuf.RpcChannel, com.google.protobuf.BlockingRpcChannel {

	/**
	 * The remote peer information. 
	 * 
	 * On the server side, this is the name of the {@link DuplexTcpClientBootstrap#getClientInfo()}
	 * peer connected to the server.
	 * 
	 * On the client side, this is the name of the peer used in {@link DuplexTcpClientBootstrap#peerWith(PeerInfo)}.
	 * 
	 * @return the remote peer.
	 */
	public PeerInfo getPeerInfo();
	
	/**
	 * Create an RPC controller for handling an individual call. 
	 * Note, the controller can only be re-used after reset() is called.
	 * 
	 * @return a new RPC controller.
	 */
	public ClientRpcController newRpcController();
	
	/**
	 * When the underlying channel closes, all pending calls
	 * must fail. Otherwise blocking calls will block forever.
	 */
	public void close();
	
	/**
	 * Return the clients underlying Netty Pipeline.
	 * 
	 * On a client, the RpcClient can customize the Netty Pipeline directly after the 
	 * {@link DuplexTcpClientBootstrap#peerWith(java.net.InetSocketAddress)} completes.
	 * 
	 * On a server, the use registered {@link RpcConnectionEventNotifier} or {@link TcpConnectionEventListener} 
	 * on the {@link DuplexTcpServerBootstrap} to get notified of new or reconnecting clients,
	 * allowing to customize the Netty pipeline. This method
	 * also works for clients {@link DuplexTcpClientBootstrap}.
	 * 
	 * @return the low level Netty ChannelPipeline.
	 */
	public ChannelPipeline getPipeline();
	
	/**
	 * Send a message to the remote peer asynchronously, out-of-band with
	 * respect to any ongoing RPC calls. Will cause the onUnsolicitedMessageCallback
	 * called on the remote peer's RpcClient assuming the {@link } is set.
	 * 
	 * @param message
	 */
	public ChannelFuture sendOobMessage( Message message );
	
	/**
	 * Allow to register a callback for unsolicited messages from the server.
	 * Non correlated messages are messages which are not related to any ongoing RPC
	 * call. 
	 * 
	 * Only a single oobMessageListener is allowed at a time. Setting the responsePrototype 
	 * or oobMessageListener parameter to null will "switch off" the onMessage reception within the
	 * client. This does not prohibit servers sending uncorrelated messages to a client which
	 * wastes bandwidth and ignores the messages.
	 * 
	 * @param responsePrototype the prototype of the messages which can be handled out-of-band from the server.
	 * @param oobMessageListener a callback function when an unsolicited messages is received from the server.
	 */
	public void setOobMessageCallback( Message responsePrototype, RpcCallback<? extends Message> oobMessageListener );
}
