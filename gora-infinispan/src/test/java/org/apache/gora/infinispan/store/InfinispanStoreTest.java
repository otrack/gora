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
import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.SingleFieldValueFilter;
import org.apache.gora.infinispan.GoraInfinispanTestDriver;
import org.apache.gora.infinispan.query.InfinispanPartitionQuery;
import org.apache.gora.infinispan.query.InfinispanQuery;
import org.apache.gora.mapreduce.MapReduceTestUtils;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreTestBase;
import org.apache.gora.store.DataStoreTestUtil;
import org.apache.gora.util.TestIOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.gora.store.DataStoreFactory.GORA_CONNECTION_STRING_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test for {@link InfinispanStore}.
 */
public class InfinispanStoreTest extends DataStoreTestBase {

  private Configuration conf;
  private InfinispanStore<String,Employee> employeeDataStore;
  private InfinispanStore<String,WebPage> webPageDataStore;

  @BeforeClass
  public static void setUpClass() throws Exception {
    List<String> cacheNames = new ArrayList<>();
    cacheNames.add(Employee.class.getSimpleName());
    cacheNames.add(WebPage.class.getSimpleName());
    setTestDriver(new GoraInfinispanTestDriver(3, 3, cacheNames));
    DataStoreTestBase.setUpClass();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf = getTestDriver().getConfiguration();
    conf.set(GORA_CONNECTION_STRING_KEY,getTestDriver().connectionString());
    employeeDataStore = (InfinispanStore<String, Employee>) employeeStore;
    webPageDataStore = (InfinispanStore<String, WebPage>) webPageStore;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected DataStore<String, Employee> createEmployeeDataStore()
    throws IOException {
    throw new IllegalStateException("Using driver.");
  }

  @SuppressWarnings("unchecked")
  @Override
  protected DataStore<String, WebPage> createWebPageDataStore()
    throws IOException {
    throw new IllegalStateException("Using driver.");
  }

  @Test
  public void testReadWriteQuery() throws Exception {

    DataStoreTestUtil.populateEmployeeStore(employeeStore, 100);

    // Marshability
    InfinispanQuery<String,Employee> query = new InfinispanQuery<>(employeeDataStore);
    query.setFields("field");
    query.setKeyRange("1", "1");
    query.setLimit(1);
    query.setOffset(1);
    query.build();
    TestIOUtils.testSerializeDeserialize(query);
    assertNotNull(query.getDataStore());

    // Correct sizes
    for (int i=1; i<=100; i++) {
      query = new InfinispanQuery<>(employeeDataStore);
      query.setLimit(i);
      query.build();
      assertEquals(i, query.list().size());
    }

    // Partitioning
    employeeDataStore.setPartitionSize(10);
    query = new InfinispanQuery<>(employeeDataStore);
    for (PartitionQuery<String,Employee> q : employeeDataStore.getPartitions(query)) {
      InfinispanPartitionQuery<String,Employee> p = (InfinispanPartitionQuery) q;
      p.build();
      assertEquals(10, p.list().size());
    }

    // Test matching everything
    query = new InfinispanQuery<>(employeeDataStore);
    SingleFieldValueFilter filter = new SingleFieldValueFilter();
    filter.setFieldName("name");
    filter.setFilterOp(FilterOp.LIKE);
    List<Object> operaands = new ArrayList<>();
    operaands.add("*");
    filter.setOperands(operaands);
    query.setFilter(filter);
    query.build();
    assertEquals(100,query.list().size());

    // Test matching nothing
    query = new InfinispanQuery<>(employeeDataStore);
    filter = new SingleFieldValueFilter();
    filter.setFieldName("name");
    filter.setFilterOp(FilterOp.UNLIKE);
    operaands.clear();
    operaands.add("*");
    filter.setOperands(operaands);
    query.setFilter(filter);
    query.build();
    assertEquals(0,query.list().size());

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
