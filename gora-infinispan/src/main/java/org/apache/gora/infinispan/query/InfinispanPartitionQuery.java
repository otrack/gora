package org.apache.gora.infinispan.query;

import org.apache.gora.infinispan.store.InfinispanStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;

/**
 * A wrapper around an InfinispanQuery, making it partition-aware.
 *
 * @author Pierre Sutra, valerio schiavoni
 *
 */
public class InfinispanPartitionQuery<K,T extends PersistentBase> extends InfinispanQuery<K,T> implements PartitionQuery<K,T> {

    private static final String[] location={"local"}; // FIXME purpose of this field is unclear.

    public InfinispanPartitionQuery(){
    }

    public InfinispanPartitionQuery(InfinispanStore<K,T> store) {
        super(store);
    }

    @Override
    public String[] getLocations() {
        return location;
    }

}
