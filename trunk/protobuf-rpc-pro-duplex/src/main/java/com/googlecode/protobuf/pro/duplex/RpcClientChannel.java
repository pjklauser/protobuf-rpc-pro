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
package com.googlecode.protobuf.pro.duplex;

import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;

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
	public RpcController newRpcController();
	
	/**
	 * When the underlying channel closes, all pending calls
	 * must fail. Otherwise blocking calls will block forever.
	 */
	public void close();
	
}
