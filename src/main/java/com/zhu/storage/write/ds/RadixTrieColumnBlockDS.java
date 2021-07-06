package com.zhu.storage.write.ds;

import com.zhu.result.ColumnResult;

import java.io.IOException;
import java.io.OutputStream;

public class RadixTrieColumnBlockDS extends ColumnBlockDS  {
  @Override
  public void initBuilder() {

  }

  @Override
  public ColumnResult directGet(int rowNum) {
    return null;
  }

  @Override
  public int getType() {
    return 0;
  }

  @Override
  public void spill(OutputStream out) throws IOException {

  }

  @Override
  public int put(byte[] record, int start, int length, boolean isNull) throws IOException {
    return 0;
  }
}
