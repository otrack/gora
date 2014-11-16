/**
 a * Licensed to the Apache Software Foundation (ASF) under one
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

import org.apache.gora.infinispan.query.InfinispanPartitionQuery;
import org.apache.gora.infinispan.query.InfinispanQuery;
import org.apache.gora.infinispan.query.InfinispanResult;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.impl.DataStoreBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * {@link org.apache.gora.infinispan.store.InfinispanStore} is the primary class
 * responsible for directing Gora CRUD operations into Infinispan.
 * This class delegate most operations, e.g., initialization, creating and deleting schemas (Infinispan caches),
 * to {@link org.apache.gora.infinispan.store.InfinispanClient},
 *
 * @author Pierre Sutra, Valerio Schiavoni
 *
 */
public class InfinispanStore<K, T extends PersistentBase> extends DataStoreBase<K, T> {

  public static final Logger LOG = LoggerFactory.getLogger(InfinispanStore.class);

  /**
   * Infinispan store Parameters
   */
  public static final String PARTITION_SIZE_DEFAULT = "10000"; // for testing purposes only ( a well-known working value for M/R is 1000).
  public static final String PARTITION_SIZE_KEY = "infinispan.partition.size";

  private InfinispanClient<K, T> infinispanClient;
  private String primaryFieldName;
  private int primaryFieldPos;
  private int partitionSize;

  /**
   * The default constructor for InfinispanStore
   */
  public InfinispanStore() throws Exception {
  }

  @Override
  public synchronized void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {

    try {

      if (primaryFieldName!=null) {
        LOG.info("Client already initialized; ignoring.");
        return;
      }

      super.initialize(keyClass, persistentClass, properties);
      infinispanClient  = new InfinispanClient<>();
      infinispanClient.setConf(conf);

      LOG.info("InfinispanStore initializing with key class: "
        + keyClass.getCanonicalName()
        + " and persistent class: "
        + persistentClass.getCanonicalName());
      schema = persistentClass.newInstance().getSchema();

      primaryFieldPos = 0;
      primaryFieldName = schema.getFields().get(0).name();
      this.infinispanClient.initialize(keyClass, persistentClass, properties);

      if (properties.getProperty(PARTITION_SIZE_KEY)!=null){
        partitionSize = Integer.valueOf(properties.getProperty(PARTITION_SIZE_KEY));
      } else {
        partitionSize = Integer.valueOf(
          conf.get(PARTITION_SIZE_KEY,
            getConf().get(PARTITION_SIZE_KEY,PARTITION_SIZE_DEFAULT)));
      }

      LOG.info("Partition query size set to "+partitionSize);

    } catch (Exception e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    LOG.debug("close");
    flush();
  }

  @Override
  public void createSchema() {
    LOG.debug("creating Infinispan cache");
    this.infinispanClient.createCache();
  }

  @Override
  public boolean delete(K key) {
    this.infinispanClient.deleteByKey(key);
    return true;
  }

  @Override
  public long deleteByQuery(Query<K, T> query) {
    InfinispanQuery<K, T> q = (InfinispanQuery) query;
    q.build();
    for( T t : q.list()){
      infinispanClient.deleteByKey((K) t.get(primaryFieldPos));
    }
    return q.getResultSize();
  }

  @Override
  public void deleteSchema() {
    LOG.debug("delete schema");
    this.infinispanClient.dropCache();
  }

  /**
   * When executing Gora Queries in Infinispan .. TODO
   */
  @Override
  public Result<K, T> execute(Query<K, T> query) {
    return new InfinispanResult<K, T>(this, (InfinispanQuery<K,T>)query);
  }

  @Override
  public T get(K key){
    return infinispanClient.get(key);
  }

  @Override
  public boolean containsKey(K key){
    return infinispanClient.containsKey(key);
  }

  @Override
  public T get(K key, String[] fields) {

    if (fields==null)
      return infinispanClient.get(key);

    InfinispanQuery query = new InfinispanQuery(this);
    query.setKey(key);
    query.setFields(fields);
    query.build();

    List<T> l = query.list();
    assert l.isEmpty() || l.size()==1;
    if (l.isEmpty())
      return null;

    return l.get(0);

  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
    throws IOException {

    List<PartitionQuery<K,T>> partitionQueries = new ArrayList<>();
    InfinispanPartitionQuery<K,T> partitionQuery;

    int cacheSize = getClient().getCache().size();
    long limit = query.getLimit();
    long size = limit>0 ? Math.min((long)cacheSize,limit) : cacheSize;
    for(int i=0; i<size/partitionSize; i++) {

      // build query
      partitionQuery = new InfinispanPartitionQuery<>((InfinispanQuery<K, T>) query);
      partitionQuery.setOffset(i * partitionSize);
      partitionQuery.setLimit(partitionSize);
      partitionQuery.build();

      // add to the list
      partitionQueries.add(partitionQuery);

      // if last remove limit
      if (i+partitionSize >= cacheSize)
        partitionQuery.setLimit(-1);

    }

    if (partitionQueries.size()==0) {
      partitionQuery = new InfinispanPartitionQuery<>((InfinispanQuery<K, T>) query);
      partitionQuery.build();
      partitionQueries.add(partitionQuery);
    }

    return partitionQueries;
  }

  @Override
  public void flush() {
    LOG.info("No caching done yet.");
  }

  /**
   * In Infinispan, Schemas are referred to as caches.
   *
   * @return Cache
   */
  @Override
  public String getSchemaName() {
    return this.infinispanClient.getCacheName();
  }

  @Override
  public Query<K, T> newQuery() {
    Query<K, T> query = new InfinispanQuery<K, T>(this);
    query.setFields(getFieldsToQuery(null));
    return query;
  }

  @Override
  public void put(K key, T obj) {

    LOG.info("put " + key.toString() + "=>" + obj.toString());

    if (obj.get(primaryFieldPos)==null)
      obj.put(primaryFieldPos,key);

    if (!obj.get(primaryFieldPos).equals(key) )
      LOG.warn("Invalid or different primary field !");

    this.infinispanClient.put(key, obj);
  }

  @Override
  public void putIfAbsent(K key, T obj){

    LOG.info("putIfabsent " + key.toString() + "=>" + obj.toString());

    if (obj.get(primaryFieldPos)==null)
      obj.put(primaryFieldPos,key);

    if (!obj.get(primaryFieldPos).equals(key) )
      LOG.warn("Invalid or different primary field !");

    this.infinispanClient.putifabsent(key, obj);

  }

  @Override
  public boolean schemaExists() {
    LOG.info("schema exists");
    return infinispanClient.cacheExists();
  }

  public InfinispanClient<K, T> getClient() {
    return infinispanClient;
  }

  public String getPrimaryFieldName() {
    return primaryFieldName;
  }

  public void setPrimaryFieldName(String name){
    primaryFieldName = name;
  }

  public int getPrimaryFieldPos(){
    return primaryFieldPos;
  }

  public void setPrimaryFieldPos(int p){
    primaryFieldPos = p;
  }

  public void setPartitionSize(int i) {
    partitionSize = i;
  }
}
