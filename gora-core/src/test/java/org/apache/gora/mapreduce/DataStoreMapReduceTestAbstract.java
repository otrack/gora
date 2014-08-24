package org.apache.gora.mapreduce;

import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class DataStoreMapReduceTestAbstract extends HadoopTestCase {

    public static final Logger LOG = LoggerFactory.getLogger(DataStoreMapReduceTestAbstract.class);

    protected JobConf job;

    public DataStoreMapReduceTestAbstract(int mrMode, int fsMode, int taskTrackers,
                                      int dataNodes) throws IOException {
        super(mrMode, fsMode, taskTrackers, dataNodes);
    }

    public DataStoreMapReduceTestAbstract() throws IOException {
        this(HadoopTestCase.CLUSTER_MR, HadoopTestCase.DFS_FS, 2, 2);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        LOG.info("Setting up Hadoop Test Case...");
        try {
            super.setUp();
            job = createJobConf();
        } catch (Exception e) {
            LOG.error("Hadoop Test Case set up failed", e);
            // cleanup
            tearDown();
        }
    }

    @Override
    public void tearDown() throws Exception {
        LOG.info("Tearing down Hadoop Test Case...");
        super.tearDown();
    }
}
