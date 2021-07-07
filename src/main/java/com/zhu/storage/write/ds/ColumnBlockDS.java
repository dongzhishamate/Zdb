package com.zhu.storage.write.ds;

import com.zhu.result.ColumnResult;
import com.zhu.serde.ZdbType;
import com.zhu.storage.write.ds.metrics.ColumnMetrics;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class ColumnBlockDS {

  //load后的内存直接映射
  ByteBuffer dataBuffer;

  protected int rowNum = 0;
  protected int dicSize = 0;
  boolean isClosed = false;
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

  public void load(ByteBuffer buffer, int len) {
    dataBuffer = buffer.slice();
    buffer.position(buffer.position() + len);
  }

  public void setRowNum(int rowNum) {
    this.rowNum = rowNum;
  }

  public abstract void initBuilder();

  public abstract ColumnResult directGet(int rowNum);

  public abstract int getType();

  public abstract void spill(OutputStream out) throws IOException;

  public abstract int put(byte[] record, int start, int length, boolean isNull) throws IOException;

  public void setColumnMetrics(ColumnMetrics cm) {
    this.cm = cm;
  }

  public ColumnMetrics getColumnMetrics() {
    return cm;
  }

  public int getRowNum() {
    return rowNum;
  }


  public int getDicSize() {
    return dicSize;
  }

  public boolean isCompressed(int compressCodec) {
    return compressCodec > 0;
  }

  public void close() {
    isClosed = true;
  }

  // Reuse column block
  public boolean canReuse() {
    return false;
  }

  public void reuse() {
    isClosed = false;
  }
}
