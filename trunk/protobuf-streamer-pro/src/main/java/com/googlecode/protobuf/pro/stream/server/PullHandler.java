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
import com.googlecode.protobuf.pro.stream.TransferOut;

/**
 * @author Peter Klauser
 *
 */
public interface PullHandler<E extends Message> {

	public E getPrototype();
	
	public void handlePull( E message, TransferOut transferOut );
}
