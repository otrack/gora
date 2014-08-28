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
import org.apache.gora.mapreduce.MapReduceTestUtils;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.DataStoreTestBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * Test for {@link InfinispanStore}.
 */
public class InfinispanStoreTest extends DataStoreTestBase {

  private Configuration conf;

  static {
    setTestDriver(new GoraInfinispanTestDriver());
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf = getTestDriver().getConf();
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

}
