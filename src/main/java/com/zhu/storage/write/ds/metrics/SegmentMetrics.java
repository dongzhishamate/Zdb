package com.zhu.storage.write.ds.metrics;

import java.util.ArrayList;

public class SegmentMetrics extends ZdbMetrics {
  public int blockNum;
  public int rowNum;
  public ArrayList<RowBlockMetrics> rowBlockMetrics = new ArrayList<RowBlockMetrics>();

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Block Number: ").append(blockNum).append('\n');
    sb.append("Row Number: ").append(rowNum).append('\n');
    for (int i = 0; i < rowBlockMetrics.size(); i++) {
      sb.append("ROW BLOCK #").append(i).append('\n');
      sb.append(rowBlockMetrics.get(i)).append('\n');
    }
    return sb.toString();
  }

  public void doStatistics() {
    for (int i = 0; i < rowBlockMetrics.size(); i++) {
      rowBlockMetrics.get(i).doStatistics();
      dataRead += rowBlockMetrics.get(i).dataRead;
    }
  }
}
