package com.googlecode.protobuf.pro.duplex.test.ssl;

import static org.junit.Assert.assertNotNull;

import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import org.junit.Before;
import org.junit.Test;

import com.googlecode.protobuf.pro.duplex.RpcSSLContext;

public class CertificateValidationTest {

	RpcSSLContext client;
	RpcSSLContext server;

	@Before
	public void setUp() throws Exception {
		client = new RpcSSLContext();
		client.setKeystorePath("ssl/client.keystore");
		client.setKeystorePassword("changeme");
		client.setTruststorePath("ssl/truststore");
		client.setTruststorePassword("changeme");
		client.init();
		assertNotNull(client.createClientEngine());

		server = new RpcSSLContext();
		server.setKeystorePath("ssl/server.keystore");
		server.setKeystorePassword("changeme");
		server.setTruststorePath("ssl/truststore");
		server.setTruststorePassword("changeme");
		server.init();
		assertNotNull(server.createServerEngine());
	}

	@Test
	public void testServerCheck() throws CertificateException,
			NoSuchAlgorithmException, CertPathValidatorException,
			InvalidAlgorithmParameterException {
		X509Certificate[] clientchain = null;
		KeyManager[] clientKeyManagers = client.getKeyManagers();
		for (KeyManager km : clientKeyManagers) {
			if (km instanceof X509KeyManager) {
				X509KeyManager xkm = (X509KeyManager) km;
				clientchain = xkm.getCertificateChain("client-key");
			}
		}
		assertNotNull(clientchain);

		X509Certificate[] serverchain = null;
		KeyManager[] serverKeyManagers = server.getKeyManagers();
		for (KeyManager km : serverKeyManagers) {
			if (km instanceof X509KeyManager) {
				X509KeyManager xkm = (X509KeyManager) km;
				serverchain = xkm.getCertificateChain("server-key");
			}
		}
		assertNotNull(serverchain);

		X509TrustManager t = null;
		X509Certificate[] rootCAs = null;
		TrustManager[] tms = server.getTrustManagers();
		for (TrustManager tm : tms) {
			if (tm instanceof X509TrustManager) {
				t = (X509TrustManager) tm;
				rootCAs = t.getAcceptedIssuers();
				break;
			}
		}

		assertNotNull(t);
		assertNotNull(rootCAs);
		t.checkClientTrusted(clientchain, "RSA");

		CertificateFactory cf = CertificateFactory.getInstance("X509");
		CertPathValidator validator = CertPathValidator.getInstance("PKIX");
		List<X509Certificate> certs = new ArrayList<>();
		for (X509Certificate c : clientchain) {
			certs.add(c);
		}
		;
		CertPath cp = cf.generateCertPath(certs);
		PKIXParameters pkixParameters;
		Set<TrustAnchor> tas = new HashSet<>();
		for (X509Certificate c : rootCAs) {
			TrustAnchor ta = new TrustAnchor(c, null);
			tas.add(ta);
		}
		;
		pkixParameters = new PKIXParameters(tas);
		pkixParameters.setRevocationEnabled(false);
		validator.validate(cp, pkixParameters);
	}

}
