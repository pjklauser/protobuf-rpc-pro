package com.googlecode.protobuf.pro.duplex.test.ssl;

import junit.framework.TestCase;

import com.googlecode.protobuf.pro.duplex.RpcSSLContext;

public class RpcSSLContextTest extends TestCase {

	
	public void testLoad() throws Exception {
		RpcSSLContext ctx = new RpcSSLContext();
		
		ctx.setKeystorePath("com/googlecode/protobuf/pro/duplex/test/ssl/keystore");
		ctx.setKeystorePassword("pwd");
		
		ctx.setTruststorePath("com/googlecode/protobuf/pro/duplex/test/ssl/truststore");
		ctx.setTruststorePassword("changeme");
		
		ctx.init();
		
		assertNotNull(ctx.createClientEngine());
		assertNotNull(ctx.createServerEngine());
	}
}
