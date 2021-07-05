package com.zhu.storage.column.block.decoder;

import com.zhu.utils.OptimizedBitSet;

import java.nio.ByteBuffer;

public class RowKeyColumnDecoderV2 extends RowKeyColumnDecoderV1 {

  OptimizedBitSet deleteMark = new OptimizedBitSet();
  public RowKeyColumnDecoderV2(ByteBuffer dataBuffer) {
    super(dataBuffer);
  }

  @Override
  public void decode() {
    super.decode();
    deleteMark.load(dataBuffer);
  }

  @Override
  public OptimizedBitSet getDeleteMark() {
    return deleteMark;
  }

}
