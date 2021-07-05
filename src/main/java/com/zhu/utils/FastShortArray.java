package com.zhu.utils;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.storage.write.ds.RowBlockDS;
import com.zhu.storage.write.ds.metrics.ColumnMetrics;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class FastShortArray implements Iterable<Short> {
  protected int MaxCompacity = Short.MAX_VALUE * 2;
  protected int capacity = 10;
  protected int size = 0;
  protected short lastPutValue;
  protected int rlRepeat = 0;

  protected ByteBuffer data;

  protected short[] elementData;
  int[] batchForReused;

  public FastShortArray() {
    elementData = new short[capacity];
  }

  public void setBatchResued(int[] resued) {
    this.batchForReused = resued;
  }


  public FastShortArray(short[] value) {
    this(value, value.length);
  }

  public FastShortArray(short[] value, int length) {
    elementData = value;
    size = length;
    capacity = length;
  }

  public void clear() {
    size = 0;
    rlRepeat = 0;
    lastPutValue = 0;
  }

  public FastShortArray(int length) {
    elementData = new short[length];
    capacity = length;
    size = length;
  }

  public short[] getArray() {
    short[] result = new short[size];
    System.arraycopy(elementData, 0, result, 0, size);
    return result;
  }

  public short[] getArrayRef() {
    return elementData;
  }

  public void add(short value) {
    if (value != lastPutValue) {
      lastPutValue = value;
      rlRepeat++;
    }
    elementData = growIfNeed(elementData, 1);
    elementData[size] = value;
    size++;
  }

  private short[] growIfNeed(short[] oldArray, int num) {
    if (size + num >= capacity) {
      capacity = ((capacity*3)/2 + 1) >= (capacity + num) ? ((capacity*3)/2 + 1) : (capacity + num);
      if (capacity >= MaxCompacity) {
        capacity = MaxCompacity;
      }
      short[] newArray = new short[capacity];
      System.arraycopy(oldArray, 0, newArray, 0, size);
      return newArray;
    }
    return oldArray;
  }

  public void add(short[] value) {
    elementData = growIfNeed(elementData, value.length);
    System.arraycopy(value, 0, elementData, size, value.length);
    size += value.length;
  }

  public void set(int index, short element) {
    elementData[index] = element;
  }

  public int get(int lineIndex) {
    return unsignedShort(elementData[lineIndex]);
  }

  public static int unsignedShort(short s) {
    return ((int)s & 0xFFFF);
  }

  public int[] getBatch(int[] lineIndexes, int effectiveLength) {
    int[] batch;
    if (effectiveLength <= RowBlockDS.BATCH_SIZE) {
      batch = batchForReused;
    } else {
      batch = new int[effectiveLength];
    }
    for (int i = 0; i < effectiveLength; i++) {
      batch[i] = get(lineIndexes[i]);
    }
    return batch;
  }

  public int size() {
    return size;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < size; i++) {
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(elementData[i]);
    }
    return sb.toString();
  }

  @Override
  public Iterator<Short> iterator() {
    return new Iterator<Short>() {
      int count = 0;

      @Override
      public boolean hasNext() {
        return (count < size);
      }

      @Override
      public Short next() {
        return elementData[count++];
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public void spill(DataOutputStream dos, ColumnMetrics cm) throws IOException {
    dos.writeShort(size());
    int beforeEncoding = dos.size();
    byte[] b = FastSerdeHelper.serShortBatch(getArray(), size);
    dos.write(b);
    int afterEncoding = dos.size();
    cm.dictionarySize += afterEncoding - beforeEncoding + 2;
  }

  public void loadInternal(int size) {
    short[] rowLengthArray = FastSerdeHelper.deShortBatch(data, data.position(), size);
    data.position(data.position() + (size << 1));
    elementData = rowLengthArray;
    capacity = this.size = size;
  }

  public void load(ByteBuffer buffer) {
    int size = buffer.getShort() & 0xFFFF;
    this.data = buffer.slice();
    buffer.position(buffer.position() + (size << 1));
    loadInternal(size);
  }
}
