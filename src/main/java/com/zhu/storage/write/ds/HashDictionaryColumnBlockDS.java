package com.zhu.storage.write.ds;

import com.zhu.result.ByteArrayColumnResult;
import com.zhu.result.ColumnResult;
import com.zhu.result.DataColumn;
import com.zhu.storage.write.ds.metrics.ColumnMetrics;
import com.zhu.utils.OptimizedBitSet;
import com.zhu.utils.RowIndexForOffset;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class HashDictionaryColumnBlockDS extends ColumnBlockDS {

  ArrayList<ByteArrayColumnResult> rawData;
  ArrayList<Short> rows;
  OptimizedBitSet nullValuePosition;
  //记录列中数据类型不同情况下每个值的偏移量
  RowIndexForOffset rowOffset;
  int recordLength = -1;
  protected DataColumn loadData = null;

  @Override
  public void initBuilder() {
    rawData = new ArrayList<>();
    rows = new ArrayList<>();
    nullValuePosition = new OptimizedBitSet();
    cm = new ColumnMetrics();
  }

  @Override
  public int getType() {
    return ColumnBlockDSFactory.ColumnBlockType.HashDict.ordinal();
  }

  public int put(byte[] record, int start, int length) {
    return put(record, start, length, false);
  }

  public int put(byte[] record, int start, int length, boolean isNull) {
    int idx;
    if(recordLength == -1) {
      recordLength = length;
    } else if (recordLength != length) {
      recordLength = -2;
    }
    if(isNull) {
      nullValuePosition.add(rowNum);
    }
    idx = putRawData(record);
    rowNum++;
    return idx;
  }

  private int putRawData(byte[] record) {
    ByteArrayColumnResult columnResult = new ByteArrayColumnResult(record, 0, record.length, dataType);
    rawData.add(columnResult);
    return rawData.size() - 1;
  }

  public ColumnResult directGet(int rowNum) {
    decode(true);
    return loadData.directGet(rowNum);
  }

  public void spill(OutputStream out) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);
    nullValuePosition.spill(dos, cm);
    spillDictData(dos, rawData);
  }

  private void spillDictData(DataOutputStream dos, ArrayList<ByteArrayColumnResult> dictData) throws IOException {
    int dicSize = dictData.size();
    dos.writeInt(dicSize);
    FastByteArrayOutputStream dic = new FastByteArrayOutputStream();
    int offLength = 0;
    //存数据类型的长度
    dos.writeInt(recordLength);
    for(int i = 0; i < dicSize; i++) {
      ByteArrayColumnResult row = dictData.get(i);
      dic.write(row.getData(), row.getOffset(), row.getLength());
      offLength += row.getLength();
    }
    //总的数据的偏移量
    dos.writeInt(offLength);
    dos.write(dic.array, 0, dic.length);
    dos.flush();
    dic.close();
  }


  public void decode(boolean copyToHead) {
    decodeColumn(null, copyToHead);
  }

  private void decodeColumn(int[] index, boolean copyToHead) {
    initFastReadDataIndex(dataBuffer, index, true);
  }

  private void initFastReadDataIndex(ByteBuffer dataBuffer, int[] index, boolean copyToHeap) {
    nullValuePosition = new OptimizedBitSet();
    nullValuePosition.load(dataBuffer);
    dicSize = dataBuffer.getInt();
    rowNum = dicSize;
    int columnLength = dataBuffer.getInt();
    decodeDataColumn(true, columnLength, dicSize, copyToHeap);
  }

  private void decodeDataColumn(boolean b, int columnLength, int dictSize, boolean copyToHeap) {
    //可能是每个值的偏移量，由于这边都一样，所以这个值暂时为null
    rowOffset = null;
    //拿到总的数据的偏移量
    int dataLength = dataBuffer.getInt();
    //fastData这个结构有点复杂，考虑暂时用别的结构代替
    loadData = new DataColumn(columnLength, dictSize, dataType);
    loadData.load(dataBuffer, dataLength);
  }
}
