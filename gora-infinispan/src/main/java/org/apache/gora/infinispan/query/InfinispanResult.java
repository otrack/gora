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
 * TODO implements a lazy retrieval of the results (if possible over HotRod).
 *
 */
public class InfinispanResult<K, T extends PersistentBase> extends ResultBase<K, T>  {

    public static final Logger LOG = LoggerFactory.getLogger(InfinispanResult.class);

    private List<T> list;
    private int current;
    private int primaryFieldPos;

	public InfinispanResult(DataStore<K, T> dataStore, InfinispanQuery<K, T> query) {
        super(dataStore, query);
        query.build();
        list = query.list();
        current = 0;
        primaryFieldPos = ((InfinispanStore<K,T>)dataStore).getPrimaryFieldPos();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
        return (float)1;
	}

	@Override
	protected boolean nextInner() throws IOException {
        if(current==list.size())
            return false;
        persistent = list.get(current);
        key = (K) list.get(current).get(primaryFieldPos);
        current++;
        return true;
    }

}
