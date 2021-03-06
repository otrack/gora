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
package org.apache.gora.filter;

import org.apache.gora.examples.generated.WebPage;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import static org.junit.Assert.*;

public class TestMapFieldValueFilter {

  @Test
  public void testSerialization() throws IOException {
    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<String, WebPage>();
    //set filter field name as metadata
    filter.setFieldName(WebPage.Field.METADATA.toString());
    filter.setMapKey("fetchTime");
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.getOperands().add("http://example.org");
    byte[] byteArray = WritableUtils.toByteArray(filter);
    MapFieldValueFilter<String, WebPage> filter2 = new MapFieldValueFilter<String, WebPage>();
    filter2.readFields(new DataInputStream(new ByteArrayInputStream(byteArray)));
    assertEquals(filter.getFieldName(), filter2.getFieldName());
    assertEquals(filter.getMapKey(), filter2.getMapKey());
    assertEquals(filter.getFilterOp(), filter2.getFilterOp());
    assertArrayEquals(filter.getOperands().toArray(), filter2.getOperands().toArray());
    assertEquals(filter.isFilterIfMissing(), filter2.isFilterIfMissing());
  }
  
  @Test
  public void testFilterBasics() {
    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<String, WebPage>();
    //set filter field name as outlinks
    filter.setFieldName(WebPage.Field.OUTLINKS.toString());
    filter.setMapKey("example");
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.getOperands().add("http://example.org");
    
    WebPage page = WebPage.newBuilder().build();
    page.getOutlinks().put("example", "http://example.org");
    assertFalse(filter.filter("irrelevant", page));
    page.getOutlinks().put("example", "http://example2.com");
    assertTrue(filter.filter("irrelevant", page));
    page = new WebPage();
    assertTrue(filter.filter("irrelevant", page));
    filter.setFilterIfMissing(false);
    
    assertFalse(filter.filter("irrelevant", page));
  }
  
  @Test
  public void testFilterEntryInMap() {
    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<String, WebPage>();
    //set filter field name as outlinks
    filter.setFieldName(WebPage.Field.OUTLINKS.toString());
    filter.setMapKey("foobar.whatever");
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.getOperands().add("Click here for foobar!");
    
    WebPage page = WebPage.newBuilder().build();
    assertTrue(filter.filter("irrelevant", page));
    page.getOutlinks().put("foobar.whatever", "Mismatch!");
    assertTrue(filter.filter("irrelevant", page));
    page.getOutlinks().put("foobar.whatever",
      "Click here for foobar!");
    assertFalse(filter.filter("irrelevant", page));
  }

}
