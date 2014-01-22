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
package com.googlecode.protobuf.pro.duplex.util;

import java.util.concurrent.ThreadFactory;

/**
 * Provides a means to Proxy a ThreadFactory and rename the ThreadFactory's threads.
 * 
 * @author Peter Klauser
 *
 */
public class RenamingThreadFactoryProxy implements ThreadFactory {

	private final ThreadFactory delegate;
	private final String namePrefix;

	public RenamingThreadFactoryProxy( String namePrefix, ThreadFactory delegate ) {
		if ( namePrefix == null ) {
			throw new IllegalArgumentException("namePrefix");
		}
		if ( delegate == null ) {
			throw new IllegalArgumentException("delegate");
		}
		this.delegate = delegate;
		this.namePrefix = namePrefix;
	}
	
	@Override
	public Thread newThread(Runnable r) {
		Thread t = delegate.newThread(r);
		
		try {
			t.setName(namePrefix + ":" + t.getName());
		} catch ( SecurityException e ) {
			// just ignore
		}
		return t;
	}
	
}
