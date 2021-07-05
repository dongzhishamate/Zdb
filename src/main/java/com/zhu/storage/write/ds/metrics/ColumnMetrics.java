package com.zhu.storage.write.ds.metrics;

public class ColumnMetrics extends ZdbMetrics {
  public long itemSize;
  public long indexSize;
  public long dictionarySize;

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(itemSize).append(", ").append(dictionarySize).append("d, ").append(indexSize).append("i");
    return sb.toString();
  }
}
