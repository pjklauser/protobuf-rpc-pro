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
package com.googlecode.protobuf.pro.duplex.logging;

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.duplex.PeerInfo;

/**
 * A RpcLogger implementation which does not log anything.
 * 
 * @author Peter Klauser
 * 
 */
public class NullLogger implements RpcLogger {

	@Override
	public void logCall(PeerInfo client, PeerInfo server, String signature,
			Message request, Message response, String errorMessage,
			int correlationId, long requestTS, long responseTS) {
	}

	@Override
	public void logOobResponse(PeerInfo client, PeerInfo server,
			Message message, String signature, int correlationId, long eventTS) {
	}

	@Override
	public void logOobMessage(PeerInfo client, PeerInfo server,
			Message message, long eventTS) {
	}


}
