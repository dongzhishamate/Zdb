package com.zhu.result;

import java.nio.ByteBuffer;

public class DataColumn {

  private ByteBuffer dataBuffer;
  private ByteArrayColumnResult byteReused;
  private int columnLength;
  private int dataSize;
  private int dataType;
  private byte[] reusedByteArray;

  public DataColumn(int columnLength, int dataSize, int dataType) {
    this.columnLength = columnLength;
    this.dataSize = dataSize;
    this.dataType = dataType;
    byteReused = new ByteArrayColumnResult(null, 0, 0, -1);
    reusedByteArray = new byte[columnLength];
  }

  public ColumnResult directGet(int dicNum) {
    int absoluteOffset = dicNum * columnLength;
    dataBuffer.position(absoluteOffset);
    dataBuffer.get(reusedByteArray);
    ByteArrayColumnResult.reuse(byteReused, reusedByteArray,0 ,columnLength);
    return byteReused;
  }


  public void load(ByteBuffer buffer, int length) {
    dataBuffer = buffer.slice();
    buffer.position(buffer.position() + length);
  }
}
