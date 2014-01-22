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
package com.googlecode.protobuf.pro.duplex.listener;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;


/**
 * TcpConnectionEventListener is informed when TCP
 * connections are opened and closed to a remote peer.
 * 
 * @author Peter Klauser
 *
 */
public interface TcpConnectionEventListener {

	/**
	 * Notification that a RpcClientChannel has closed. This
	 * happens when a remote peer closes an open RpcClientChannel
	 * or when the TCP connection on which the RpcClientChannel
	 * is built up on breaks - due to JVM crash / kill or network
	 * problem. The underlying reason for RpcClientChannel closure is not
	 * discernible.
	 * 
	 * @param client
	 */
	public void connectionClosed( RpcClientChannel clientChannel );
	
	/**
	 * Notification that a RpcClientChannel has been opened. This
	 * happens once a TCP connection is opened and the RPC handshake
	 * is successfully completed.
	 * 
	 * @param clientChannel
	 */
	public void connectionOpened( RpcClientChannel clientChannel );
	
}
