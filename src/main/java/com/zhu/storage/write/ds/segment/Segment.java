package com.zhu.storage.write.ds.segment;

import com.zhu.storage.write.ds.RowBlockDS;
import com.zhu.storage.write.ds.metrics.RowBlockMetrics;
import com.zhu.storage.write.ds.metrics.SegmentMetrics;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class Segment {

  public static int MaxSize = 256 * 1024 * 1024; // 256M
  private SegmentMetrics sm = new SegmentMetrics();
  protected int compressCodec;
  protected int blockSize = 0;
  protected RowBlockDS currentBlock = null;
  protected File file = null;
  protected RandomAccessFile raf = null;
  protected int effectiveLength = 0;
  int colnum = 0;
  protected int[] columnBlockTypes = null;
  // Optional value, set this will improve perforamce and comrpession ratio.
  protected int[] columnDataTypes = null;
  protected int[] columnDataLengths = null;
  private int currentInMemoryRows = 0;
  private int currentInMemorySize = 0;

  public Segment(int[] columnBlockTypes, int[] columnDataTypes, int[] columnDataLength, File file) {
    this.file = file;
    this.columnBlockTypes = columnBlockTypes;
    this.columnDataTypes = columnDataTypes;
    this.columnDataLengths = columnDataLength;
  }

  public SegmentMetrics getSegmentMetrics() {
    return sm;
  }

  public boolean isFull() {
    return effectiveLength > MaxSize;
  }

  protected RowBlockDS getOrCreateCurrentBlock() throws IOException {
    if (currentBlock == null || currentBlock.isFull()) {
      if (currentBlock != null) {
        currentBlock.close();
        flush(currentBlock);
        if (sm != null) {
          sm.blockNum++;
          RowBlockMetrics rbm = new RowBlockMetrics();
          sm.rowBlockMetrics.add(rbm);
          currentBlock.setRowBlockMetrics(rbm);
        }
        //复用节约资源，暂时还未实现
        currentBlock.reuse();
        currentInMemoryRows = 0;
        currentInMemorySize = 0;
      } else {
        RowBlockMetrics rbm = null;
        if (sm != null) {
          sm.blockNum++;
          rbm = new RowBlockMetrics();
          sm.rowBlockMetrics.add(rbm);
        }
        currentBlock = new RowBlockDS(columnBlockTypes, columnDataTypes, columnDataLengths, rbm);
        currentBlock.setCompressCodec(compressCodec);
        currentBlock.initBuilder(colnum);
      }
      currentBlock.setBlockNum(blockSize);
      blockSize++;
    }
    return currentBlock;
  }

  protected abstract void flush(RowBlockDS block) throws IOException;

  public abstract void close(boolean closeCurrentBlock) throws IOException;

  final public void close() throws IOException {
    close(true);
  }

  public SegmentMetrics getMetrics() {
    return this.sm;
  }

  public void put(byte[][] row) throws IOException {
    put(row, false);
  }

  public void put(byte[][] row, boolean containsRowKey) throws IOException {
    put(row, containsRowKey, false);
  }

  public void put(byte[][] row, boolean containsRowKey, boolean isDelete) throws IOException {
    return;
  }

  public int getEffectiveLength() {
    return effectiveLength;
  }

  public int getCurrentInMemorySize() {
    return currentInMemorySize;
  }
}
