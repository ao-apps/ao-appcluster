/*
 * ao-appcluster - Application-level clustering tools.
 * Copyright (C) 2011, 2015, 2016  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-appcluster.
 *
 * ao-appcluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-appcluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-appcluster.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.appcluster;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author  AO Industries, Inc.
 */
public class PropertiesConfigurationTest {

	@Test
	public void testRealWorldConfig() throws IOException, AppClusterConfigurationException, InterruptedException {
		URL url = PropertiesConfigurationTest.class.getResource("appcluster.properties");
		AppClusterPropertiesConfiguration config;
		if(url.getProtocol().equals("file")) {
			config = new AppClusterPropertiesConfiguration(new File(url.getPath()));
		} else {
			Properties props = new Properties();
			try (InputStream in = url.openStream()) {
				props.load(in);
			}
			config = new AppClusterPropertiesConfiguration(props);
		}
		AppCluster cluster = new AppCluster(config);
		cluster.addResourceListener(new LoggerResourceListener());
		cluster.start();
		try {
			assertEquals("Production Cluster", cluster.getDisplay());
			Thread.sleep(60000);
		} finally {
			cluster.stop();
		}
	}
}
