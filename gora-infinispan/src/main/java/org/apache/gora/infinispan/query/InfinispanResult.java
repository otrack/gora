package org.apache.gora.infinispan.query;

import org.apache.gora.infinispan.store.InfinispanStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/*
 * @author Pierre Sutra, valerio schiavoni
  *
 */
public class InfinispanResult<K, T extends PersistentBase> extends ResultBase<K, T>  {

  public static final Logger LOG = LoggerFactory.getLogger(InfinispanResult.class);

  private List<T> list;
  private int current;
  private int primaryFieldPos;
//  private String primaryFieldName;
//  private List<String> fields;

  public InfinispanResult(DataStore<K, T> dataStore, InfinispanQuery<K, T> query) {
    super(dataStore, query);
    list = query.list();
    current = 0;
    primaryFieldPos = ((InfinispanStore<K,T>)dataStore).getPrimaryFieldPos();
//    primaryFieldName = ((InfinispanStore<K,T>)dataStore).getPrimaryFieldName();
//    fields = new ArrayList<>();
//    if (query.getFields()!=null &&
//      (query.getFields().length!=1 || !query.getFields()[0].equals(primaryFieldName)) ) { // projection required
//      for (String field : query.getFields())
//        fields.add(field);
//    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    LOG.debug("getProgress()");
    if (list.size()==0) return 1;
    float progress = ((float)current/(float)list.size());
    LOG.trace("progress: "+progress);
    return progress;
  }

  @Override
  protected boolean nextInner() throws IOException {
    LOG.debug("nextInner()");
    if(current==list.size()) {
      LOG.trace("end");
      return false;
    }
    persistent = list.get(current);
    key = (K) list.get(current).get(primaryFieldPos);
//    T shell = list.get(current);
//    key = (K) shell.get(primaryFieldPos);
//    assert key!=null;
//    persistent = dataStore.get(key);
//    if (!fields.isEmpty()) {
//      T projection = (T) persistent.newInstance();
//      for (Schema.Field field : persistent.getSchema().getFields()) {
//        if (fields.contains(field.name())) {
//          int position = persistent.getSchema().getField(field.name()).pos();
//          Object value = persistent.get(position);
//          projection.put(position,value);
//        }
//      }
//      persistent = projection;
//    }
    current++;
    return true;
  }

  public int size() {
    return list.size();
  }
  
  @Override
  protected void clear() {
    LOG.debug("clear()");
    // do nothing
  }

}
