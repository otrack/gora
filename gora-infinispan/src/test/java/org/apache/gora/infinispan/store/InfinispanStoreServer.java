package org.apache.gora.infinispan.store;

import org.apache.gora.infinispan.GoraInfinispanTestDriver;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;

/**
 * // TODO: Document this
 *
 * @author otrack
 * @since 4.0
 */
public class InfinispanStoreServer {


    private static Configuration conf;
    private static GoraInfinispanTestDriver testDriver;

    static{
        try {
            testDriver = new GoraInfinispanTestDriver();
            testDriver.setUpClass();
            conf = testDriver.getConfiguration();
        } catch (Exception e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
    }

    public InfinispanStoreServer(){}

    @Test
    @Ignore
    public void runServer(){
        System.out.println("STARTING SERVER");
        synchronized(this){
            try{
                this.wait();
            }catch(InterruptedException e){
                // ignore this'
            } catch (Exception e) {
                e.printStackTrace();  // TODO: Customise this generated block
            } finally {
                try {
                    testDriver.tearDownClass();
                } catch (Exception e) {
                    e.printStackTrace();  // TODO: Customise this generated block
                }
            }
        }
    }
}
