package org.apache.gora.infinispan.query;

import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.filter.SingleFieldValueFilter;
import org.apache.gora.infinispan.store.InfinispanClient;
import org.apache.gora.infinispan.store.InfinispanStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.impl.QueryBase;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;


/*
 * * @author Pierre Sutra, valerio schiavoni
 */
public class InfinispanQuery<K, T extends PersistentBase> extends QueryBase<K, T> {

  public static final Logger LOG = LoggerFactory.getLogger(InfinispanQuery.class);

  private QueryFactory qf;
  private org.infinispan.query.dsl.Query q;
  private QueryBuilder qb;
  private String primaryFieldName;

  public InfinispanQuery(){
    super(null);
  }

  public InfinispanQuery(InfinispanStore<K, T> dataStore) {
    super(dataStore);

  }

  public void build(){

    FilterConditionContext context = null;

    if(qb==null)
      init();

    if(q!=null)
      throw new IllegalAccessError("Query already built; ignoring");

    if (filter instanceof  MapFieldValueFilter){
      MapFieldValueFilter mfilter = (MapFieldValueFilter) filter;
      if (mfilter.getOperands().size()>1)
        throw new IllegalAccessError("MapFieldValueFilter operand not supported.");
      String value = mfilter.getMapKey()+"::"+mfilter.getOperands().get(0).toString();
      switch (mfilter.getFilterOp()) {
        case EQUALS:
          context = qb.having(mfilter.getFieldName()).eq(value);
          break;
        case NOT_EQUALS:
          context = qb.not().having(mfilter.getFieldName()).eq(value);
          break;
        default:
          throw new IllegalAccessError("FilterOp not supported..");
      }

    } else if (filter instanceof  SingleFieldValueFilter){
      SingleFieldValueFilter sfilter = (SingleFieldValueFilter) filter;
      if (sfilter.getOperands().size()>1)
        throw new IllegalAccessError("SingleFieldValueFilter operand not supported.");
      Object value = sfilter.getOperands().get(0);
      switch (sfilter.getFilterOp()) {
        case EQUALS:
          context = qb.having(sfilter.getFieldName()).eq(value);
          break;
        case NOT_EQUALS:
          context = qb.not().having(sfilter.getFieldName()).eq(value);
          break;
        case LESS:
          context = qb.having(sfilter.getFieldName()).lt(value);
          break;
        case LESS_OR_EQUAL:
          context = qb.having(sfilter.getFieldName()).lte(value);
          break;
        case GREATER:
          context = qb.having(sfilter.getFieldName()).gt(value);
          break;
        case GREATER_OR_EQUAL:
          context = qb.having(sfilter.getFieldName()).gte(value);
          break;
        default:
          throw new IllegalAccessError("FilterOp not supported..");
      }

    } else if (filter!=null) {
      throw new IllegalAccessError("Filter not supported.");
    }

    if (this.startKey==this.endKey && this.startKey != null ){
      (context == null ? qb : context.and()).having(primaryFieldName).eq(this.startKey);
    }else{
      if (this.startKey!=null && this.endKey!=null)
        context = (context == null ? qb : context.and()).having(primaryFieldName).between(this.startKey,this.endKey);
      else if (this.startKey!=null)
        context = (context == null ? qb : context.and()).having(primaryFieldName).between(this.startKey,null);
      else if (this.endKey!=null)
        (context == null ? qb : context.and()).having(primaryFieldName).between(null,this.endKey);
    }

    qb.maxResults((int) this.getLimit());

    String []fieldsWithPrimary = Arrays.copyOf(fields, fields.length + 1);
    fieldsWithPrimary[fields.length] = primaryFieldName;
    qb.setProjection(fieldsWithPrimary);

    qb.startOffset(this.getOffset());

    q = qb.build();
  }

  public List<T> list(){
    if(q==null)
      throw new IllegalAccessError("Build before list.");
    return q.list();
  }

  public int getResultSize(){
    return q.getResultSize();
  }

  private void init(){
    InfinispanClient<K,T> client = ((InfinispanStore<K,T>)dataStore).getClient();
    primaryFieldName = ((InfinispanStore<K,T>)dataStore).getPrimaryFieldName();
    RemoteCache<K,T> remoteCache = client.getCache();
    qf = Search.getQueryFactory(remoteCache);
    qb = qf.from(dataStore.getPersistentClass());
  }

}
