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
