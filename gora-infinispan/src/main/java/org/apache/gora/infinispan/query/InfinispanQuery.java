package org.apache.gora.infinispan.query;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.filter.SingleFieldValueFilter;
import org.apache.gora.infinispan.store.InfinispanStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.impl.QueryBase;
import org.apache.hadoop.io.WritableUtils;
import org.infinispan.client.hotrod.impl.avro.AvroQueryBuilder;
import org.infinispan.client.hotrod.impl.avro.AvroRemoteQuery;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.remote.client.avro.AvroSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/*
 * * @author Pierre Sutra
 */
public class InfinispanQuery<K, T extends PersistentBase> extends QueryBase<K, T> implements
  PartitionQuery<K,T>, Cloneable{

  public static final Logger LOG = LoggerFactory.getLogger(InfinispanQuery.class);
  private static final String ADDR_DELIMITATOR = ";";

  private AvroRemoteQuery q;
  private InetSocketAddress location;

  public InfinispanQuery(){
    super(null);
    this.localFilterEnabled = false;
    this.setOffset(-1);
  }

  public InfinispanQuery(InfinispanStore<K, T> dataStore) {
    super(dataStore);
    this.localFilterEnabled = false;
    this.setOffset(-1);
  }
  
  public boolean isBuilt() {
    LOG.debug("isBuilt()");
    return q!=null;
  }

  public void rebuild(){
    LOG.debug("rebuild()");
    q=null;
    build();
  }
  
  public void build(){
    LOG.debug("build()");

    FilterConditionContext context = null;

    if(q!=null) {
      LOG.trace("Query already built; ignoring.");
      return;
    }

    AvroQueryBuilder qb = ((InfinispanStore<K,T>)dataStore).getClient().getQueryBuilder();

    if (filter instanceof  MapFieldValueFilter){
      MapFieldValueFilter mfilter = (MapFieldValueFilter) filter;
      if (!(mfilter.getMapKey() instanceof String))
        throw new IllegalAccessError("Invalid map key, must be a string.");
      if (mfilter.getOperands().size()>1)
        throw new IllegalAccessError("MapFieldValueFilter operand not supported.");
      if (!(mfilter.getOperands().get(0) instanceof String))
        throw new IllegalAccessError("Invalid operand, must be a string.");
      String value = mfilter.getMapKey()+ AvroSupport.DELIMITER+mfilter.getOperands().get(0).toString();
      switch (mfilter.getFilterOp()) {
        case EQUALS:
          context = qb.having(mfilter.getFieldName()).eq(value);
          if (!((MapFieldValueFilter) filter).isFilterIfMissing()) {
            LOG.warn("Forcing isFilterMissing to true");
            ((MapFieldValueFilter) filter).setFilterIfMissing(true);
          }
          break;
        case NOT_EQUALS:
          context = qb.not().having(mfilter.getFieldName()).eq(value);
          if (!((MapFieldValueFilter) filter).isFilterIfMissing()) {
            LOG.warn("Forcing isFilterMissing to false");
            ((MapFieldValueFilter) filter).setFilterIfMissing(false);
          }
          break;
        case LIKE:
          context = qb.having(mfilter.getFieldName()).like(value);
          break;
        case UNLIKE:
          context = qb.not().having(mfilter.getFieldName()).like(value);
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
        case LIKE:
          if (!(value instanceof String))
            throw new IllegalAccessError("Invalid operand, it must be a string.");
          context = qb.having(sfilter.getFieldName()).like((String)value);
          break;
        case UNLIKE:
          if (!(value instanceof String))
            throw new IllegalAccessError("Invalid operand, it must be a string.");
          context = qb.not().having(sfilter.getFieldName()).like((String)value);
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

    // if projection enabled, keep the primary field.
    if (fields!=null && fields.length > 0) {
      List<String> fieldsList = new ArrayList<>(Arrays.asList(fields));
      if (!fieldsList.contains(getPrimaryFieldName())) {
        String[] fieldsWithPrimary = Arrays.copyOf(fields, fields.length + 1);
        fieldsWithPrimary[fields.length] = getPrimaryFieldName();
        qb.setProjection(fieldsWithPrimary);
      }
    }
    
    qb.orderBy(
      (getSortingField()==null) ? getPrimaryFieldName() : getSortingField(),
      isSortingAscendant() ? SortOrder.ASC : SortOrder.DESC);

    if (this.getOffset()>=0)
      qb.startOffset(this.getOffset());

    if (this.getLimit()>0)
      qb.maxResults((int) this.getLimit());

    q = (AvroRemoteQuery) qb.build();
    
    if (location!=null)
      q.setLocation(location);
  }

  public List<T> list(){
    LOG.debug("list()");
    if (!isBuilt()) build();
    return q.list();

  }
  
  public List<PartitionQuery<K,T>> split() {
    LOG.debug("split()");
    if(!isBuilt()) build();
    List<PartitionQuery<K,T>> splits = new ArrayList<>();
    AvroQueryBuilder qb = ((InfinispanStore<K,T>)dataStore).getClient().getQueryBuilder();
    Collection<AvroRemoteQuery> avroQueries = qb.split(this.q);
    for (AvroRemoteQuery avroQuery : avroQueries) {
      InfinispanQuery<K,T> split = (InfinispanQuery<K, T>) this.clone();
      split.q = avroQuery;
      split.location = avroQuery.getLocation();
      splits.add(split);
    }
    LOG.trace(splits.toString());
    return splits;
  } 
  
  public int getResultSize(){
    return q.getResultSize();
  }
  
  public String getPrimaryFieldName(){ return ((InfinispanStore)dataStore).getPrimaryFieldName();}

  @Override
  public Object clone() {
    InfinispanQuery<K,T> query = null;
    try {
      query = (InfinispanQuery<K, T>) super.clone();
    } catch (CloneNotSupportedException e) {
      // not reachable.
    }
    query.setDataStore(this.getDataStore());
    query.setFilter(this.getFilter());
    query.setFields(this.getFields());
    query.setKeyRange(this.getStartKey(), this.getEndKey());
    query.setConf(this.getConf());
    query.setStartTime(this.getStartTime());
    query.setEndTime(this.getEndTime());
    query.setLocalFilterEnabled(this.isLocalFilterEnabled());
    query.setLimit(this.getLimit());
    query.setOffset(this.getOffset());
    query.setQueryString(this.getQueryString());
    query.setSortingField(this.getSortingField());
    query.setSortingOrder(this.isSortingAscendant());
    query.q = this.q;
    query.location = this.location;
    return query;
  }

  @Override
  public String[] getLocations() {
    if (location==null)
      return new String[0];
    String[] result = new String[1];
    result[0] = location.getHostString();
    return result;
  }
  
  public InetSocketAddress getLocation(){
    return location;
  }
  
  // FIXME use the write non-null fields function.

  @Override
  @SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    String locationString = WritableUtils.readString(in);
    if (!locationString.equals(""))
      location = new InetSocketAddress(
        locationString.split(ADDR_DELIMITATOR)[0],
        Integer.valueOf(locationString.split(ADDR_DELIMITATOR)[1]));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    if (location!=null)
      WritableUtils.writeString(out,location.getHostName()+ADDR_DELIMITATOR+location.getPort());
    else
      WritableUtils.writeString(out,"");
  }
  
  @Override
  public String toString() {
    ToStringBuilder builder = new ToStringBuilder(this);
    builder.append("dataStore", dataStore);
    builder.append("location", location==null ? null :location.toString());
    builder.append("fields", fields);
    builder.append("startKey", startKey);
    builder.append("endKey", endKey);
    builder.append("filter", filter);
    builder.append("limit", limit);
    builder.append("offset", offset);
    builder.append("localFilterEnabled", localFilterEnabled);
    return builder.toString();
  }

}
