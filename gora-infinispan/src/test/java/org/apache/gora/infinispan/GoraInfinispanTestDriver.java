/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author valerio schiavoni
 *
 */

package org.apache.gora.infinispan;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.apache.gora.GoraTestDriver;
import org.apache.gora.examples.generated.Employee;
import org.apache.gora.examples.generated.WebPage;
import org.apache.gora.infinispan.store.InfinispanStore;
import org.apache.hadoop.conf.Configuration;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
// Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for third party tests using gora-infinispan backend.
 * 
 * @see GoraTestDriver for test specifics. This driver is the base for all test
 *      cases that require an embedded Infinispan server. It starts (setUp) and
 *      stops (tearDown) embedded Infinispan server.
 * 
 */

public class GoraInfinispanTestDriver extends GoraTestDriver {
	private static Logger log = LoggerFactory
			.getLogger(GoraInfinispanTestDriver.class);
	private InfinispanServerStarter infinispanServerInitializer;

	public GoraInfinispanTestDriver() {
		super(InfinispanStore.class);
	}

	/**
	 * Starts embedded Infinispan server.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Override
	public void setUpClass() throws Exception {
		super.setUpClass();
		log.info("Starting embedded Infinispan Server...");

		infinispanServerInitializer = new InfinispanServerStarter();
		try {
			infinispanServerInitializer.createCacheManagers();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stops embedded Infinispan server.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Override
	public void tearDownClass() throws Exception {
		super.tearDownClass();
		
	}

	class InfinispanServerStarter extends MultiHotRodServersTest {
		private static final int NCACHES = 1;

		@Override
		protected void createCacheManagers() throws Throwable {
			ConfigurationBuilder defaultClusteredCacheConfig = getDefaultClusteredCacheConfig(
					CacheMode.REPL_SYNC, false);
			
			ConfigurationBuilder builder = hotRodCacheConfiguration(defaultClusteredCacheConfig);
			
			createHotRodServers(NCACHES, builder);
			for (EmbeddedCacheManager m : cacheManagers) {				
				m.defineConfiguration(String.class.getCanonicalName(), builder.build());
				m.defineConfiguration(Employee.class.getCanonicalName(), builder.build());
				m.defineConfiguration(WebPage.class.getCanonicalName(), builder.build());
			}
			
			

		}

	}

	public Configuration getConf() {
		Configuration c = new Configuration();
		return c;
	}
}
