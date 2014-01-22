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
package com.googlecode.protobuf.pro.duplex;


public interface LocalCallVariableHolder {

	/**
	 * Store a call local variable.
	 * @param key
	 * @param value
	 * @return any previously stored call local variable with the same key.
	 */
	public Object getCallLocalVariable( String key );
	
	/**
	 * Fetch a previously stored call local variable.
	 * @param key
	 * @param value
	 * @return a previously stored call local variable.
	 */
	public Object storeCallLocalVariable( String key, Object value );
}
