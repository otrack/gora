package org.apache.gora.infinispan.store;

import org.apache.gora.infinispan.GoraInfinispanTestDriver;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


/**
 *
 * @author Pierre Sutra
 * @since 7.0
 */
public class InfinispanStoreServer {


  private static Configuration conf;
  private static GoraInfinispanTestDriver testDriver;

  static{
    try {
      testDriver = new GoraInfinispanTestDriver(3);
      testDriver.setUpClass();
      conf = testDriver.getConfiguration();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public InfinispanStoreServer(){}

  @Test
  public void runServer(){
    System.out.println("STARTING SERVER");
    synchronized(this){
      try{
        this.wait();
      }catch(InterruptedException e){
        // ignore this'
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          testDriver.tearDownClass();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
}
