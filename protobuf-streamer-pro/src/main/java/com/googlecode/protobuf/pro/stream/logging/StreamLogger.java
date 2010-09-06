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
package com.googlecode.protobuf.pro.stream.logging;

import java.util.Map;

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.stream.PeerInfo;

/**
 * Interface of a Logging facility which logs each streamed transfer at completion time.
 * 
 * @author Peter Klauser
 *
 */
public interface StreamLogger {

	public void logTransfer( PeerInfo client, PeerInfo server, Message request, String errorMessage, int correlationId, Map<String,String> parameters, long startTS, long endTS );
	
}
