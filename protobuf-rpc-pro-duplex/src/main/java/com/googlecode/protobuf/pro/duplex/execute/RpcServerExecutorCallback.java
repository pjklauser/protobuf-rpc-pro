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
package com.googlecode.protobuf.pro.duplex.execute;

import com.google.protobuf.Message;


/**
 * @author Peter Klauser
 *
 */
public interface RpcServerExecutorCallback {

	/**
	 * Called once after the RPC server call finishes ( or fails ).
	 * An RPC server call which is canceled will not be called back.
	 * 
	 * All NotifyOnCancel notification has been done by the 
	 * RpcServerCallExecutor.
	 * 
	 * @param correlationId
	 * @param message Message of RPC response, or null if controller indicates error.
	 */
	public void onFinish( int correlationId, Message message );
	
}
