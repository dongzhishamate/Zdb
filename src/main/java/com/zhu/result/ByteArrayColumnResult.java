package com.zhu.result;

import com.zhu.common.KeyType;

public class ByteArrayColumnResult extends ColumnResult {
  private byte[] data;
  private int offset;
  private int length;

  public ByteArrayColumnResult(byte[] data, int dataType) {
    super(dataType);
    this.data = data;
    this.offset = 0;
    this.length = data.length;
  }

  public ByteArrayColumnResult(byte[] data, int offset, int length, int dataType) {
    super(dataType);
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  public byte getByte(int idx) {
    return data[offset + idx];
  }

  public byte[] getData() {
    return data;
  }

  public int getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  public byte[] toArray() {
    if(isNull()) {
      return null;
    } else {
      byte[] temp = new byte[length];
      System.arraycopy(data, offset, temp, 0, length);
      return temp;
    }
  }

  public boolean isNull() {
    return data == null && offset == -1 && length == -1;
  }

  public static ByteArrayColumnResult createNullColumnResult() {
    return new ByteArrayColumnResult(null, -1, -1, KeyType.UNKNOWN);
  }
}
