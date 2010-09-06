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
package com.googlecode.protobuf.pro.stream.server;

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.stream.TransferIn;

/**
 * @author Peter Klauser
 *
 */
public interface PushHandler<E extends Message> {

	public E getPrototype();
	
	/**
	 * The initial Push message is received before any data chunks
	 * The PushHandler has the opportunity to initialize data structures
	 * to handle the Push and get ready for {@link #data(Message, TransferIn)}
	 * to be called for each received chunk.
	 * 
	 * The transferIn will not have any data available.
	 * 
	 * @param message
	 */
	public void init( E message, TransferIn transferIn );
	
	/**
	 * New chunk data is available in TransferIn.
	 * 
	 * Once transfer from client to server is finished, the server
	 * {@link TransferIn#isOpen()} will return false. The method will
	 * never be called again once the TransferIn returns false.
	 *  
	 * 
	 * @param message
	 * @param transferIn
	 */
	public void data( E message, TransferIn transferIn );
	
	/**
	 * Cleanup and perform tranfer end processing once transfer has ended.
	 * 
	 * The success or failure of the transfer is not considered here
	 * this must be determined by data integrity information transferred
	 * with the TODO tranferIn properties
	 *  
	 * The transferIn will be closed.
	 * @param message
	 * @param transferIn
	 */
	public void end( E message, TransferIn transferIn );
}
