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

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;


/**
 * RpcConnectionEventListeners are informed when RpcClientChannels
 * to a remote peer connect, reconnect, changes and disconnect from the
 * local peer. Connect is when a connection is established to a previously
 * unknown peer. Reconnect is when a connection is established to a
 * known peer, and the process id of the peer is the same as was known.
 * Change is when a connection is established to a known peer, but the PID of the
 * peer has changed ( ie. it has stopped/crashed and restarted ).
 *  
 * Note: in order to utilize the differentiating between "reconnection" and "change"
 * you will need to set the System property "pid" explicitly when starting the
 * JVM. More precisely it is not possible (currently) for running JAVA process
 * to ascertain it's PID ( unless using native calls ). It is possible to pass
 * a PID of a startup script to a child JAVA process as a "-Dpid=$$".
 * {@linkplain http://blog.igorminar.com/2007/03/how-java-application-can-discover-its.html}
 * 
 * @author Peter Klauser
 *
 */
public interface RpcConnectionEventListener {

	/**
	 * The RPC channel to a remote peer has closed. This can be
	 * due to a clean {@link RpcClientChannel#close()} or if the
	 * underlying TCP connection breaks - due to network problems
	 * or JVM crash / kill. The reason for channel closure is not
	 * discernible.
	 * 
	 * @param clientChannel
	 */
	public void connectionLost( RpcClientChannel clientChannel );
	
	/**
	 * An RPC channel to a new remote peer has been opened. The
	 * remote peer has not been connected to before.
	 * 
	 * @param clientChannel
	 */
	public void connectionOpened( RpcClientChannel clientChannel );
	
	/**
	 * An RPC channel which was previously lost has reconnected, and it's PID
	 * is the same as before - ie. it is the same running instance as before.
	 * 
	 * If the RPC peer exhibits the same {@link PeerInfo#getPid()} then the same
	 * instance is reconnected, otherwise {@link #connectionChanged(RpcClientChannel)}
	 * would be notified instead.
	 * 
	 * @param clientChannel
	 */
	public void connectionReestablished( RpcClientChannel clientChannel );

	/**
	 * An RPC channel which was previously lost has reconnected, and it's PID
	 * is not the same as before - ie. it is not the same running instance as before.
	 * If the System property "pid" is not set, then it is defaulted to "NONE", in 
	 * which case {@link #connectionReestablished(RpcClientChannel)} will always be
	 * issued.
	 * 
	 * If the RPC peer exhibits the same {@link PeerInfo#getPid()} then the same
	 * instance is reconnected, otherwise {@link #connectionChanged(RpcClientChannel)}
	 * would be notified instead.
	 * 
	 * @param clientChannel
	 */
	public void connectionChanged( RpcClientChannel clientChannel );
}
