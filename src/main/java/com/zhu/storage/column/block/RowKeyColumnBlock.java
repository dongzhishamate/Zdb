package com.zhu.storage.column.block;

public class RowKeyColumnBlock {

  private RowKeyColumnReader rowKeyColumnReader;
  private RowKeyColumnWriter rowKeyColumnWriter;

  public RowKeyColumnBlock() {
    rowKeyColumnWriter = new RowKeyColumnWriter();
  }

  public void put(byte[] offset, boolean isDelete) {
    rowKeyColumnWriter.put(offset, isDelete);
  }

  public void put(byte[] rowSetId, byte[] offset) {
    rowKeyColumnWriter.put(rowSetId, offset);
  }

  public void put(byte[] rowSetId, byte[] offset, boolean isDelete) {
    rowKeyColumnWriter.put(rowSetId, offset, isDelete);
  }

}
