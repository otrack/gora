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

package org.apache.gora.infinispan.store;

import org.apache.avro.Schema;
import org.apache.gora.persistency.impl.PersistentBase;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.query.remote.client.avro.AvroMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/*
 * * @author Pierre Sutra, valerio schiavoni
 */
public class InfinispanClient<K, T extends PersistentBase> {

    public static final Logger LOG = LoggerFactory.getLogger(InfinispanClient.class);

    private static final String HOTROD_DEFAULT_HOST = "127.0.0.1";
    private static final String HOTROD_DEFAULT_PORT = "15233";

    private Class<K> keyClass;
	private Class<T> persistentClass;
	private RemoteCacheManager cacheManager;
    private Schema schema;

	private RemoteCache<K, T> cache;
    private boolean cacheExists;

    public void initialize(Class<K> keyClass, Class<T> persistentClass,
                           Properties properties) throws Exception {

		LOG.info("Initializing InfinispanClient");

        String host = System.getProperty("hotrod_host",HOTROD_DEFAULT_HOST);
        int port = Integer.valueOf(System.getProperty("hotrod_port", HOTROD_DEFAULT_PORT));

        this.keyClass = keyClass;
		this.persistentClass = persistentClass;
        this.schema = persistentClass.newInstance().getSchema();

		ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
		clientBuilder
                .addServer()
                .host(host)
                .port(port)
                .marshaller(new AvroMarshaller<T>(persistentClass));
		cacheManager = new RemoteCacheManager(clientBuilder.build(), true);
        cacheManager.start();
        cache = cacheManager.getCache(); // FIXME
	}

	public boolean cacheExists() {
		return cacheExists;
	}

	/**
	 * Check if cache already exists. If not, create it.
	 */
	public void createCache() {
        cacheExists = true; // FIXME
	}

	/**
	 * Drop keyspace.
	 */
	public void dropCache() {
        cacheExists = false; // FIXME
        cache.clear();
	}

	public void deleteByKey(K key) {
        cache.remove(key);
    }

	public void putInCache(K key, T val) {
		this.cache.put(key, val);
	}

    public T getInCache(K key){
        return cache.get(key);
    }

	public String getCacheName() {
		return this.keyClass.getName();
	}

	public RemoteCacheManager getCacheManager() {
		return cacheManager;
	}

	public void setCacheManager(RemoteCacheManager cacheManager) {
		this.cacheManager = cacheManager;
	}

	public RemoteCache<K, T> getCache() {
		return this.cache;
	}

}
