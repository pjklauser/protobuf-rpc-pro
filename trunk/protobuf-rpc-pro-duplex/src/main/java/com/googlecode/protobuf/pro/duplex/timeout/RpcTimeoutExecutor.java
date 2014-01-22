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
package com.googlecode.protobuf.pro.duplex.timeout;

import java.util.concurrent.ExecutorService;

import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.RpcServer;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.RpcCancel;
import com.googlecode.protobuf.pro.duplex.wire.DuplexProtocol.RpcError;


/**
 * @author Peter Klauser
 *
 */
public interface RpcTimeoutExecutor extends ExecutorService {

	/**
	 * Timeout a client side RPC call.
	 * 
	 */
	public void timeout( RpcClient rpcClient, RpcError rpcError );
	
	/**
	 * Timeout a server side RPC call.
	 * 
	 */
	public void timeout( RpcServer rpcServer, RpcCancel rpcCancel );
	
}
