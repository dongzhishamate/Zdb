package com.zhu.storage.column.block;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.storage.column.block.decoder.RowKeyColumnDecoder;
import com.zhu.storage.column.block.decoder.RowKeyColumnDecoderV0;
import com.zhu.storage.column.block.decoder.RowKeyColumnDecoderV1;
import com.zhu.storage.column.block.decoder.RowKeyColumnDecoderV2;
import com.zhu.utils.OptimizedBitSet;

import java.nio.ByteBuffer;

public class RowKeyColumnReader {

  private enum RowKeyVersion {
    V0,
    V1,
    V2 // add delete mark
  }

  boolean rowBlockContainsRowKeyColumn;
  ByteBuffer buffer;
  int rowKeyColumnLength;
  int[] offsets;
  long[] rowSetIds;
  OptimizedBitSet deleteMark;
  boolean isDecode = false;
  private RowKeyVersion rowKeyVersion = RowKeyVersion.V2;

  public RowKeyColumnReader(boolean rowBlockContainsRowKeyColumn, ByteBuffer buffer) {
    this.rowBlockContainsRowKeyColumn = rowBlockContainsRowKeyColumn;
    this.buffer = buffer;
  }

  public void loadRowKeyColumn() {
    rowKeyVersion = RowKeyVersion.values()[FastSerdeHelper.deInt(buffer, buffer.position())];
    buffer.position(buffer.position() + FastSerdeHelper.IntSize);
    deCompressRowKeyColumn(rowKeyColumnLength - FastSerdeHelper.IntSize);
  }

  public void decode(ByteBuffer dataBuffer) {
    if (rowBlockContainsRowKeyColumn) {
      buffer = dataBuffer.slice();
      rowKeyColumnLength = FastSerdeHelper.deInt(buffer, 0);
      buffer.position(buffer.position() + 4);
      dataBuffer.position(dataBuffer.position() + 4 + rowKeyColumnLength);
    }
  }

  private void deCompressRowKeyColumn(int dataLength) {
    RowKeyColumnDecoder decoder;
    if (rowKeyVersion == RowKeyVersion.V0) {
      decoder = new RowKeyColumnDecoderV0(buffer, dataLength);
    } else if (rowKeyVersion == RowKeyVersion.V1) {
      decoder = new RowKeyColumnDecoderV1(buffer);
    } else if (rowKeyVersion == RowKeyVersion.V2) {
      decoder = new RowKeyColumnDecoderV2(buffer);
    } else {
      throw new RuntimeException("Unknown rowKey column version");
    }
    decoder.decode();
    offsets = decoder.getOffsets();
    rowSetIds = decoder.getRowSetIds();
    deleteMark = decoder.getDeleteMark();
  }


}
