package com.zhu.utils;

import com.zhu.storage.write.ds.metrics.ColumnMetrics;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class OptimizedBitSet implements Iterable<Integer> {

  private SimpleBitSet nullValueBitSet;
  private FastShortArray nullValuePos;
  private int flag;

  private boolean isClear = false;

  private static final int RATIO = 1000;
  private static final int BS = 1;
  private static final int SHORTARRAY = 2;

  private int nullValueSize = 0;

  public OptimizedBitSet() {
    nullValuePos = new FastShortArray();
    nullValueBitSet = new SimpleBitSet();
  }

  public void add(int pos) {
    if (!onlyUseBitSet()) {
      nullValuePos.add((short) pos);
      nullValueBitSet.set(pos);
    } else {
      flag = BS;
      nullValueBitSet.set(pos);
      if (!isClear) {
        nullValuePos = null;
        isClear = true;
      }
    }
    nullValueSize++;
  }

  private boolean onlyUseBitSet() {
    return nullValueSize > RATIO;
  }

  public void spill(DataOutputStream dos, ColumnMetrics cm) throws IOException {
    if (onlyUseBitSet()) {
      dos.write(BS);
      nullValueBitSet.spill(dos);
    } else {
      dos.write(SHORTARRAY);
      nullValuePos.spill(dos, cm);
    }
  }

  public void load(ByteBuffer buffer) {
    flag = buffer.get();
    if (flag == BS) {
      nullValueBitSet.load(buffer);
    } else {
      nullValuePos.load(buffer);
      nullValueBitSet = new SimpleBitSet();
      for (int i = 0; i < nullValuePos.size(); i++) {
        nullValueBitSet.set(nullValuePos.get(i));
      }
    }
  }

  public SimpleBitSet getNullValueBitSet() {
    return nullValueBitSet;
  }

  public boolean isSet(int rowNum) {
    return nullValueBitSet.get(rowNum);
  }

  public void clear() {
    nullValueBitSet.clear();
    if (nullValuePos != null) {
      nullValuePos.clear();
    }
  }

  public short[] getArrayRef() {
    if (flag != BS) {
      return nullValuePos.getArrayRef();
    } else {
      return nullValueBitSet.simpleBitSet2ShortArray();
    }
  }

  @Override
  public Iterator<Integer> iterator() {
    return new Iterator<Integer>() {
      int idx = 0;
      int nullIndex = 0;
      int nullValueSize = nullValueBitSet.cardinality();

      @Override
      public boolean hasNext() {
        return idx < nullValueSize;
      }

      @Override
      public Integer next() {
        int r = nullValueBitSet.nextSetBit(nullIndex);
        nullIndex = r + 1;
        idx++;
        return r;
      }

      @Override
      public void remove() {
        throw new RuntimeException("Unsupported method");
      }
    };
  }
}
