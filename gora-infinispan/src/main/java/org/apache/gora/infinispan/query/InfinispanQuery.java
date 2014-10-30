package org.apache.gora.infinispan.query;

import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.filter.SingleFieldValueFilter;
import org.apache.gora.infinispan.store.InfinispanStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.impl.QueryBase;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;


/*
 * * @author Pierre Sutra, valerio schiavoni
 */
public class InfinispanQuery<K, T extends PersistentBase> extends QueryBase<K, T> {

  public static final Logger LOG = LoggerFactory.getLogger(InfinispanQuery.class);

  private QueryBuilder qb;
  private org.infinispan.query.dsl.Query q;

  public InfinispanQuery(){
    super(null);
  }

  public InfinispanQuery(InfinispanStore<K, T> dataStore) {
    super(dataStore);
  }

  public void build(){

    FilterConditionContext context = null;

    if(qb==null)
      qb = ((InfinispanStore<K,T>)dataStore).getClient().getQueryBuilder();

    if(q!=null) {
      LOG.info("Query already built; ignoring.");
      return;
    }

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
      (context == null ? qb : context.and()).having(getPrimaryFieldName()).eq(this.startKey);
    }else{
      if (this.startKey!=null && this.endKey!=null)
        context = (context == null ? qb : context.and()).having(getPrimaryFieldName()).between(this.startKey,this.endKey);
      else if (this.startKey!=null)
        context = (context == null ? qb : context.and()).having(getPrimaryFieldName()).between(this.startKey,null);
      else if (this.endKey!=null)
        (context == null ? qb : context.and()).having(getPrimaryFieldName()).between(null,this.endKey);
    }

    qb.maxResults((int) this.getLimit());

    // if projection enabled, keep the primary field.
    if (fields!=null && fields.length > 0) {
      String[] fieldsWithPrimary = Arrays.copyOf(fields, fields.length + 1);
      fieldsWithPrimary[fields.length] = getPrimaryFieldName();
      qb.setProjection(fieldsWithPrimary);
    }

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

  public String getPrimaryFieldName(){ return ((InfinispanStore)dataStore).getPrimaryFieldName();}

}
