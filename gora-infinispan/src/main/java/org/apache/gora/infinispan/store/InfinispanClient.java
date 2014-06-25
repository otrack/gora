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

import org.apache.gora.persistency.impl.PersistentBase;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.query.remote.client.avro.AvroMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Properties;

public class InfinispanClient<K, T extends PersistentBase> {

    public static final Logger LOG = LoggerFactory.getLogger(InfinispanClient.class);

    private Class<K> keyClass;
	private Class<T> persistentClass;
	private RemoteCacheManager cacheManager;

	private RemoteCache<K, T> cache; // TODO use as types the keyClass clazz

	public void initialize(Class<K> keyClass, Class<T> persistentClass,
			Properties properties) throws Exception {

		LOG.info("Initializing InfinispanClient");

        this.keyClass = keyClass;
		this.persistentClass = persistentClass;

		ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
        AvroMarshaller<T> marshaller = new AvroMarshaller<T>(persistentClass);
		clientBuilder
                .addServer()
                .host("127.0.0.1")
                .port(15233)
                .marshaller(marshaller);
		cacheManager = new RemoteCacheManager(clientBuilder.build(), true);
        cacheManager.start();
        cache = cacheManager.getCache();
        System.out.println("HERE => "+persistentClass.getCanonicalName());
        ProtoStreamMarshaller.getSerializationContext(cacheManager)
                .registerMarshaller(persistentClass, marshaller);
	}
	
	 private static ObjectName createObjectName(String name) {
	      try {
	         return new ObjectName(name);
		} catch (MalformedObjectNameException e) {
	         throw new RuntimeException(e);
	      }
	   }


	/**
	 * Check if keyspace already exists. In the case of Infinispan, check if a
	 * cache with the same name already exists.
	 */
	public boolean keyspaceExists() {
		if (cacheManager.getCache(getKeyspaceName()) != null) {
			return true;
		}
		return false;
	}

	/**
	 * Check if keyspace already exists. If not, create it.
	 */
	public void checkKeyspace() {
		RemoteCache<Object, Object> remoteCache = cacheManager
				.getCache(getKeyspaceName());
		if (remoteCache == null) {
			// TODO there is no way via hot-rod to create a remote cache..what
			// do we do here ?
			// Pierre suggests to go via JMX
		}
	}

	/**
	 * Drop keyspace.
	 */
	public void dropKeyspace() {

		// via hot-rod we cannot delete caches, what do we do ? JMX again ?

	}

	public void deleteByKey(K key) {
		throw new UnsupportedOperationException(
				"To be implemented yet");
	}

	public void putInCache(K key, T val) {
		this.cache.put(key, val);
	}

	/**
	 * Obtain Schema/Keyspace name
	 * 
	 * @return Keyspace
	 */
	public String getKeyspaceName() {
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

    public T  getInCache(K key, String[] fields) {
        // FIXME can one implement the projection at the server side ?
        return cache.get(key);
    }

    //
    // HELPERS
    //


}
