package org.apache.gora.infinispan.mapreduce;

import org.apache.gora.infinispan.GoraInfinispanTestDriver;
import org.apache.gora.mapreduce.DataStoreMapReduceTestAbstract;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 *
 * @author Pierre Sutra
 * @since 0.5
 */
public abstract class InfinispanStoreMapReduceAbstractTest extends DataStoreMapReduceTestAbstract{

    private GoraInfinispanTestDriver driver;

    public InfinispanStoreMapReduceAbstractTest() throws IOException {
        super();
        driver = new GoraInfinispanTestDriver();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        driver.setUpClass();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        driver.tearDownClass();
    }

}
