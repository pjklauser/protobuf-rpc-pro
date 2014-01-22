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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Provides convenience functions for Resource files.
 * 
 * @author Peter Klauser
 *
 */
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
