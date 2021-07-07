package com.zhu.storage.write.ds.segment;

import com.zhu.storage.write.ds.RowBlockDS;
import com.zhu.utils.ValueUtils;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class SortedBucketMergeSegment extends WritableAppendOnlySegment {


  private long memorySize = 64 * 1024 * 1024L;
  private int rowNumSize = 64512 * 5;


  class BytesValueComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      return ValueUtils.compare(o1, 0, o1.length, o2, 0, o2.length, columnDataTypes[rowKeyColIdx]);
    }
  }

  private long curEffectiveSize = 0;
  private long curRowNum = 0;
  private int rowKeyColIdx = 0;
  private ConcurrentSkipListMap<byte[], RowData> cachedRows =
          new ConcurrentSkipListMap<byte[], RowData>(new BytesValueComparator());

  public SortedBucketMergeSegment(SegmentMeta segmentMeta,
                                  File file,
                                  int colIdx) {
    super(segmentMeta, file);
    rowKeyColIdx = colIdx;
  }

  @Override
  public boolean isFull() {
    return curEffectiveSize >= RowBlockDS.maxBlockSize || curRowNum >= RowBlockDS.MaxRecords;
  }

  @Override
  public void put(byte[][] row, boolean containsRowKey) throws IOException {
    put(row, containsRowKey, false);
  }

  @Override
  public void put(byte[][] row, boolean containsRowKey, boolean isDelete) throws IOException {
    if (row[rowKeyColIdx] == null) {
      throw new RuntimeException("rowKey column should not be null!");
    }
    cachedRows.put(row[rowKeyColIdx].clone(), new RowData(row.clone(), isDelete));
    curRowNum ++;
    for (byte[] col : row) {
      if (col != null) {
        curEffectiveSize += col.length;
      } else {
        curEffectiveSize += 4;
      }

    }
  }

  @Override
  public void close(boolean closeCurrentBlock) throws IOException {
    closeCachedRows(false);
    super.close(closeCurrentBlock);
  }

  private void closeCachedRows(boolean containsRowKey) throws IOException {
    Map.Entry<byte[], RowData> entry;
    while ((entry = cachedRows.pollFirstEntry()) != null) {
      RowData row = entry.getValue();
      RowBlockDS block = getOrCreateCurrentBlock();
      block.put(row.getRow(), containsRowKey, row.isDelete());
    }
  }

  @Override
  public int getCurrentInMemorySize() {
    return (int)curEffectiveSize;
  }

}

class RowData {
  private byte[][] row;
  private boolean isDelete = false;

  public RowData(byte[][] row, boolean isDelete) {
    this.row = row;
    this.isDelete = isDelete;
  }

  public byte[][] getRow() {
    return row;
  }

  public boolean isDelete() {
    return isDelete;
  }
}
