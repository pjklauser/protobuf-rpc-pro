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
package com.googlecode.protobuf.pro.duplex.handler;

/**
 * A listing of all used Handler's by name.
 * 
 * @author Peter Klauser
 *
 */
public abstract class Handler {

	public static final String SSL = "ssl";
	
	public static final String COMPRESSOR = "deflater";
	public static final String DECOMPRESSOR = "inflater";

	public static final String FRAME_DECODER = "frameDecoder";
	public static final String FRAME_ENCODER = "frameEncoder";
	public static final String PROTOBUF_DECODER = "protobufDecoder";
	public static final String PROTOBUF_ENCODER = "protobufEncoder";
	
	public static final String RPC_CLIENT = "rpcClient";
	public static final String RPC_SERVER = "rpcServer";
	public static final String CLIENT_CONNECT = "clientConnect";
	public static final String SERVER_CONNECT = "serverConnect";
	
}
