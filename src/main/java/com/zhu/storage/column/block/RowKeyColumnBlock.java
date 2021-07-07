package com.zhu.storage.column.block;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RowKeyColumnBlock {

  private RowKeyColumnReader rowKeyColumnReader;
  private RowKeyColumnWriter rowKeyColumnWriter;

  public RowKeyColumnBlock() {
    rowKeyColumnWriter = new RowKeyColumnWriter();
  }

  // This is for read
  public RowKeyColumnBlock(boolean rowBlockContainsRowKeyColumn, ByteBuffer buffer) {
    rowKeyColumnReader = new RowKeyColumnReader(rowBlockContainsRowKeyColumn, buffer);
  }

  public void decode(ByteBuffer dataBuffer) throws IOException {
    rowKeyColumnReader.decode(dataBuffer);
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

  public void spill(DataOutputStream dataOutputStream) throws IOException {
    rowKeyColumnWriter.spill(dataOutputStream);
  }

  public void reuse() {
    rowKeyColumnWriter.reuse();
  }
}
