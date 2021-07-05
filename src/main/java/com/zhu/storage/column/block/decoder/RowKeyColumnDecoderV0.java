package com.zhu.storage.column.block.decoder;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.utils.OptimizedBitSet;

import java.nio.ByteBuffer;

public class RowKeyColumnDecoderV0 extends RowKeyColumnDecoder {
  ByteBuffer dataBuffer;
  int dataLength;
  int[] offsets;

  public RowKeyColumnDecoderV0(ByteBuffer dataBuffer, int dataLength) {
    this.dataBuffer = dataBuffer;
    this.dataLength = dataLength;
  }

  @Override
  public void decode() {
    int rowKeySize = dataLength >> FastSerdeHelper.IntShiftSize;
    offsets = FastSerdeHelper.deIntBatch(dataBuffer, dataBuffer.position(), rowKeySize);
    dataBuffer.position(dataBuffer.position() + dataLength);
  }

  @Override
  public int[] getOffsets() {
    return offsets;
  }

  @Override
  public long[] getRowSetIds() {
    return null;
  }

  @Override
  public OptimizedBitSet getDeleteMark() {
    return new OptimizedBitSet();
  }

}
