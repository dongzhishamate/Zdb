package com.zhu.storage.column.block.decoder;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.utils.OptimizedBitSet;

import java.nio.ByteBuffer;

public class RowKeyColumnDecoderV1 extends RowKeyColumnDecoder {
  ByteBuffer dataBuffer;
  int[] offsets;
  long[] rowSetIds;

  public RowKeyColumnDecoderV1(ByteBuffer dataBuffer) {
    this.dataBuffer = dataBuffer;
  }

  @Override
  public void decode() {
    int offsetSize = dataBuffer.getInt();
    offsets = FastSerdeHelper.deIntBatch(dataBuffer, dataBuffer.position(), offsetSize);
    dataBuffer.position(dataBuffer.position() + offsetSize * 4);
    int rowSetIdSize = dataBuffer.getInt();
    rowSetIds = FastSerdeHelper.deLongBatch(dataBuffer, dataBuffer.position(), rowSetIdSize);
    dataBuffer.position(dataBuffer.position() + rowSetIdSize * 8);
  }

  @Override
  public int[] getOffsets() {
    return offsets;
  }

  @Override
  public long[] getRowSetIds() {
    return rowSetIds;
  }

  @Override
  public OptimizedBitSet getDeleteMark() {
    return new OptimizedBitSet();
  }

}
