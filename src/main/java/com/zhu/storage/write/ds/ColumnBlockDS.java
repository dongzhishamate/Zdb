package com.zhu.storage.write.ds;

import com.zhu.serde.ZdbType;
import com.zhu.storage.write.ds.metrics.ColumnMetrics;

import java.nio.ByteBuffer;

public abstract class ColumnBlockDS {

  ByteBuffer dataBuffer;

  protected int rowNum = 0;
  protected int dicSize = 0;

  protected int dataType = ZdbType.OTHER;

  protected int columnIndex = -1;

  protected ColumnMetrics cm = null;

  public int getDataType() {
    return dataType;
  }

  public void setDataType(int dataType) {
    this.dataType = dataType;
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  public void setColumnIndex(int columnIndex) {
    this.columnIndex = columnIndex;
  }

  public abstract void initBuilder();

  public void setColumnMetrics(ColumnMetrics cm) {
    this.cm = cm;
  }

  public ColumnMetrics getColumnMetrics() {
    return cm;
  }
}
