package com.zhu.storage.column.block.decoder;

import com.zhu.utils.OptimizedBitSet;

public abstract class RowKeyColumnDecoder {
  public abstract void decode();
  public abstract int[] getOffsets();
  public abstract long[] getRowSetIds();
  public abstract OptimizedBitSet getDeleteMark();
}
