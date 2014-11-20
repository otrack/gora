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
import org.apache.gora.util.ClassLoadingUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.Site;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.infinispan.ensemble.cache.distributed.ClusteringBasedPartitioner;
import org.infinispan.ensemble.cache.distributed.Coordinates;
import org.infinispan.ensemble.cache.distributed.Partitioner;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.avro.AvroMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.gora.store.DataStoreFactory.GORA_CONNECTION_STRING_DEFAULT;
import static org.apache.gora.store.DataStoreFactory.GORA_CONNECTION_STRING_KEY;

/*
 * * @author Pierre Sutra, valerio schiavoni
 */
public class InfinispanClient<K, T extends PersistentBase> implements
  Configurable{

  public static final Logger LOG = LoggerFactory.getLogger(InfinispanClient.class);
  public static final String INFINISPAN_PARTITIONER_KEY = "infinispan.partitioner.class";
  public static final String INFINISPAN_PARTITIONER_DEFAULT = "org.infinispan.ensemble.cache.distributed.HashBasedPartitioner";

  private Configuration conf;

  private Class<K> keyClass;
  private Class<T> persistentClass;
  private EnsembleCacheManager cacheManager;
  private QueryFactory qf;

  private BasicCache<K, T> cache;
  private boolean cacheExists;

  private Collection<Future> futureCollection;

  public InfinispanClient() {
    conf = new Configuration();
  }

  public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) throws Exception {

    String host = properties.getProperty(GORA_CONNECTION_STRING_KEY,
      getConf().get(GORA_CONNECTION_STRING_KEY,GORA_CONNECTION_STRING_DEFAULT));
    LOG.info("Connecting client to "+host);

    this.keyClass = keyClass;
    this.persistentClass = persistentClass;
    AvroMarshaller<T> marshaller = new AvroMarshaller<T>(persistentClass);
    cacheManager = new EnsembleCacheManager(host,marshaller);

    cache = cacheManager.getCache(
      persistentClass.getSimpleName(),
      new ArrayList<>(cacheManager.sites()),
      true,
      createPartitioner(properties));
    qf = org.infinispan.ensemble.search.Search.getQueryFactory((EnsembleCache)cache);

    futureCollection = new ArrayList<>();

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

  public void dropCache() {
    cacheExists = false; // FIXME
    cache.clear();
  }

  public void deleteByKey(K key) {
    cache.remove(key);
  }

  public synchronized void put(K key, T val) {
    // this.cache.putAsync(key, val);
    futureCollection.add(this.cache.putAsync(key, val));
  }

  public void putifabsent(K key, T obj) {
    this.cache.putIfAbsent(key,obj);
  }

  public T get(K key){
    return cache.get(key);
  }

  public boolean containsKey(K key) {
    return cache.containsKey(key);
  }

  public String getCacheName() {
    return this.persistentClass.getSimpleName();
  }

  public BasicCache<K, T> getCache() {
    return this.cache;
  }

  public QueryBuilder getQueryBuilder() {
    return qf.from(persistentClass);
  }

  public Partitioner<K,T> createPartitioner(Properties properties)
    throws ClassNotFoundException, IllegalAccessException,
    InstantiationException, NoSuchMethodException, InvocationTargetException {

    Class<Partitioner> partitionerClass = (Class<Partitioner>) ClassLoadingUtils.loadClass(
      properties.getProperty(INFINISPAN_PARTITIONER_KEY,
      getConf().get(INFINISPAN_PARTITIONER_KEY,INFINISPAN_PARTITIONER_DEFAULT)));
    Class[] parameterTypes = new Class[]{List.class};
    List<EnsembleCache<K,T>> caches = new ArrayList<>();
    for(Site site : cacheManager.sites())
         caches.add(site.<K, T>getCache(getCacheName()));
    Object[] parameters = new Object[]{caches};

    if (ClusteringBasedPartitioner.class.isAssignableFrom(partitionerClass)) {
      EnsembleCache<K, Coordinates> l = cacheManager.getCache(getCacheName());
      parameterTypes = new Class[]{List.class, EnsembleCache.class};
      parameters = new Object[]{caches,l};
    }

    return partitionerClass.getConstructor(parameterTypes).newInstance(parameters);

  }

  @Override
  public void setConf(Configuration conf) {
    this.conf =conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }


  public synchronized void flush(){
    for(Future future : futureCollection){
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    futureCollection.clear();
  }

  public void close() {
    flush();
    getCache().stop();
  }
}
