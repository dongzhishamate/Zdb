package com.zhu.result;

import java.nio.ByteBuffer;

public class DataColumn {

  private ByteBuffer dataBuffer;
  private ByteArrayColumnResult byteReused;
  private int columnLength;
  private int dataSize;
  private int dataType;

  public DataColumn(int columnLength, int dataSize, int dataType) {
    this.columnLength = columnLength;
    this.dataSize = dataSize;
    this.dataType = dataType;
    byteReused = new ByteArrayColumnResult(null, 0, 0, -1);
  }

//  public ColumnResult get(int dicNum) {
//    byteReused = new ByteArrayColumnResult(null, 0, 0, -1);
//
//  }


  public void load(ByteBuffer buffer, int length) {
    dataBuffer = buffer.slice();
    buffer.position(buffer.position() + length);
  }
}
