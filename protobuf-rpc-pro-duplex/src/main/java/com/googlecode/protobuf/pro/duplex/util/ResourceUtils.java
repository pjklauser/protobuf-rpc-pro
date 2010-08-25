package com.googlecode.protobuf.pro.duplex.util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

public class ResourceUtils {

	public static URL validateResourceURL(String path) throws Exception {
		if(  path == null ) {
			throw new IllegalArgumentException("path");
		}

		// First see if this is a URL
		try {
			return new URL(path);
		} catch (MalformedURLException e) {
			// If that didn't work, maybe it's just a file on the filesystem.
			File file = new File(path);
			if (file.exists() == true && file.isFile()) {
				return file.toURI().toURL();
			} else {
				// Otherwise it could be a resource on the classpath.
				URL url = Thread.currentThread().getContextClassLoader()
						.getResource(path);
				if (url != null)
					return url;
			}
		}
		throw new Exception("Unable to find a resource at " + path);
	}
}
