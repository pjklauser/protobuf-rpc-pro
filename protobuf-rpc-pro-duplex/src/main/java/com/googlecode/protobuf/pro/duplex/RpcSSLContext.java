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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.protobuf.pro.duplex.util.ResourceUtils;

public class RpcSSLContext {

	private static Logger log = LoggerFactory.getLogger(RpcSSLContext.class);

	private String keystoreType = "JKS";
	private String keystorePath;
	private String keystorePassword;
	
	private String truststoreType = "JKS";
	private String truststorePath;
	private String truststorePassword;
	
	private SSLContext sslContext;
	private KeyManager[] keyManagers;
	private TrustManager[] trustManagers;
	
	public RpcSSLContext() {
		
	}
	
	public void init() throws Exception {
		keyManagers = loadKeyManagers(keystoreType, keystorePath, keystorePassword);
		trustManagers = loadTrustManager(truststoreType, truststorePath, truststorePassword);
		sslContext = createSSLContext( keyManagers, trustManagers);
	}
	
	private static SSLContext createSSLContext( KeyManager[] keyManagers, TrustManager[] trustManagers) throws Exception {
		SSLContext sslContext = SSLContext.getInstance("TLS");

		// just for logging - certificate problems are a pain in the b_tt.
		for( TrustManager mgr : trustManagers ) {
			if ( mgr instanceof X509TrustManager ) {
				X509TrustManager x509mgr = (X509TrustManager)mgr;
				
				X509Certificate[] certs = x509mgr.getAcceptedIssuers();
				for( X509Certificate cert : certs ) {
					log.info("AcceptedIssuer: " + cert.getSubjectX500Principal() + ". Valid until " + cert.getNotAfter());
				}
			}
		}
		
		sslContext.init(keyManagers, trustManagers, new SecureRandom());

		return sslContext;
	}


	private static TrustManager[] loadTrustManager(String keystoreType,
			String trustStorePath, String trustStorePassword) throws Exception {
			TrustManagerFactory trustMgrFactory;
			KeyStore trustStore = RpcSSLContext.loadKeystore(keystoreType, trustStorePath,
					trustStorePassword);
			trustMgrFactory = TrustManagerFactory
					.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			trustMgrFactory.init(trustStore);
			return trustMgrFactory.getTrustManagers();
	}


	private static KeyManager[] loadKeyManagers(String keystoreType, String keystorePath,
			String keystorePassword) throws Exception {
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
				.getDefaultAlgorithm());
		KeyStore ks = loadKeystore(keystoreType, keystorePath, keystorePassword);
		kmf.init(ks, keystorePassword.toCharArray());

		return kmf.getKeyManagers();
	}
	
	private static KeyStore loadKeystore(String keystoreType, String keystorePath,
			String keystorePassword) throws Exception {
		assert keystorePath != null;
		assert keystorePassword != null;

		KeyStore ks = KeyStore.getInstance(keystoreType);
		InputStream in = null;
		try {
			URL keystoreURL = ResourceUtils.validateResourceURL(keystorePath);
			in = keystoreURL.openStream();
			ks.load(in, keystorePassword.toCharArray());
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException ignored) {
				}
			}
		}
		return ks;
	}

	/**
	 * @return the keystoreType
	 */
	public String getKeystoreType() {
		return keystoreType;
	}

	/**
	 * @param keystoreType the keystoreType to set
	 */
	public void setKeystoreType(String keystoreType) {
		this.keystoreType = keystoreType;
	}

	/**
	 * @return the keystorePath
	 */
	public String getKeystorePath() {
		return keystorePath;
	}

	/**
	 * @param keystorePath the keystorePath to set
	 */
	public void setKeystorePath(String keystorePath) {
		this.keystorePath = keystorePath;
	}

	/**
	 * @return the keystorePassword
	 */
	public String getKeystorePassword() {
		return keystorePassword;
	}

	/**
	 * @param keystorePassword the keystorePassword to set
	 */
	public void setKeystorePassword(String keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	/**
	 * @return the truststoreType
	 */
	public String getTruststoreType() {
		return truststoreType;
	}

	/**
	 * @param truststoreType the truststoreType to set
	 */
	public void setTruststoreType(String truststoreType) {
		this.truststoreType = truststoreType;
	}

	/**
	 * @return the truststorePath
	 */
	public String getTruststorePath() {
		return truststorePath;
	}

	/**
	 * @param truststorePath the truststorePath to set
	 */
	public void setTruststorePath(String truststorePath) {
		this.truststorePath = truststorePath;
	}

	/**
	 * @return the truststorePassword
	 */
	public String getTruststorePassword() {
		return truststorePassword;
	}

	/**
	 * @param truststorePassword the truststorePassword to set
	 */
	public void setTruststorePassword(String truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	/**
	 * @return the clientEngine
	 */
	public SSLEngine createClientEngine() {
		SSLEngine engine = sslContext.createSSLEngine();
		engine.setUseClientMode(true);
		engine.setWantClientAuth(true);
		return engine;
	}

	/**
	 * @return the serverEngine
	 */
	public SSLEngine createServerEngine() {
		SSLEngine engine = sslContext.createSSLEngine();
		engine.setUseClientMode(false);
		engine.setNeedClientAuth(true);
		return engine;
	}

	/**
	 * @return the keyManagers
	 */
	public KeyManager[] getKeyManagers() {
		return keyManagers;
	}

	/**
	 * @return the trustManagers
	 */
	public TrustManager[] getTrustManagers() {
		return trustManagers;
	}

}
