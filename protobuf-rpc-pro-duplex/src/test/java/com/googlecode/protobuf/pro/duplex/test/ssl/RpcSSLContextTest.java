package com.googlecode.protobuf.pro.duplex.test.ssl;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.googlecode.protobuf.pro.duplex.RpcSSLContext;

public class RpcSSLContextTest{

	
	@Test	
	public void testLoad() throws Exception {
		RpcSSLContext ctx = new RpcSSLContext();
		
		ctx.setKeystorePath("ssl/client.keystore");
		ctx.setKeystorePassword("changeme");
		
		ctx.setTruststorePath("ssl/truststore");
		ctx.setTruststorePassword("changeme");
		
		ctx.init();
		
		assertNotNull(ctx.createClientEngine());
		assertNotNull(ctx.createServerEngine());
	}
}
