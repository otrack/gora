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
 * Testing class for all standard gora-cassandra functionality.
 * We extend DataStoreTestBase enabling us to run the entire base test
 * suite for Gora. 
 */
package org.apache.gora.infinispan.store;

import org.apache.gora.examples.generated.Employee;
import org.apache.gora.examples.generated.WebPage;
import org.apache.gora.infinispan.GoraInfinispanTestDriver;
import org.apache.gora.infinispan.query.InfinispanQuery;
import org.apache.gora.mapreduce.MapReduceTestUtils;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.DataStoreTestBase;
import org.apache.gora.util.TestIOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.gora.store.DataStoreFactory.GORA_CONNECTION_STRING_KEY;
import static org.junit.Assert.assertNotNull;

/**
 * Test for {@link InfinispanStore}.
 */
public class InfinispanStoreTest extends DataStoreTestBase {

  private Configuration conf;

  static {
    List<String> cacheNames = new ArrayList<>();
    cacheNames.add(Employee.class.getSimpleName());
    cacheNames.add(WebPage.class.getSimpleName());
    setTestDriver(new GoraInfinispanTestDriver(3,cacheNames));
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf = getTestDriver().getConfiguration();
    conf.set(GORA_CONNECTION_STRING_KEY,getTestDriver().connectionString());
  }

  @SuppressWarnings("unchecked")
  @Override
  protected DataStore<String, Employee> createEmployeeDataStore()
    throws IOException {
    InfinispanStore<String, Employee> employeeDataStore =
      DataStoreFactory.getDataStore(InfinispanStore.class, String.class,Employee.class, conf);
    assertNotNull(employeeDataStore);
    employeeDataStore.initialize(String.class, Employee.class, null);
    return employeeDataStore;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected DataStore<String, WebPage> createWebPageDataStore()
    throws IOException {
    InfinispanStore<String, WebPage> webPageDataStore =
      DataStoreFactory.getDataStore(InfinispanStore.class, String.class,WebPage.class, conf);
    webPageDataStore.initialize(String.class, WebPage.class, null);
    return webPageDataStore;
  }

  @Test
  public void testReadWriteQuery() throws Exception {
    InfinispanQuery query = new InfinispanQuery((InfinispanStore) this.employeeStore);
    query.setFields("field");
    query.setKeyRange(1, 1);
    query.setLimit(1);
    query.setOffset(1);
    query.build();
    TestIOUtils.testSerializeDeserialize(query);

    assertNotNull(query.getDataStore());
  }


  @Test
  public void testCountQuery() throws Exception {
    MapReduceTestUtils.testCountQuery(webPageStore,
      testDriver.getConfiguration());
  }

  public GoraInfinispanTestDriver getTestDriver() {
    return (GoraInfinispanTestDriver) testDriver;
  }

  @Override
  @Ignore
  public void testDeleteByQueryFields() throws IOException, Exception {
    // FIXME not working
  }

  @Override
  @Ignore
  public void testDeleteByQuery() throws IOException, Exception {
    // FIXME not working
  }

  @Override
  @Ignore
  public void testQueryEndKey() throws IOException, Exception {
    // FIXME not working
  }

  @Override
  @Ignore
  public void testGetWithFields() throws IOException, Exception {
    // FIXME not working
  }


}
