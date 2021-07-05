package com.zhu.storage.write.ds.metrics;

import java.util.ArrayList;

public class RowBlockMetrics extends ZdbMetrics{
  public int rowNums = 0;
  public int rowBlockSize = 0;
  public int cubeSize = 0;
  public ArrayList<ColumnMetrics> columnMetrics = new ArrayList<ColumnMetrics>();

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Row Number: " + rowNums).append('\n');
    sb.append("Row Block Length: " + rowBlockSize).append('\n');
    sb.append("Row Cube Length: " + cubeSize).append('\n');
    for (int i = 0; i < columnMetrics.size(); i++) {
      if (i != 0) {
        sb.append(" | ");
      }
      sb.append('[').append(i).append(" : ").append(columnMetrics.get(i)).append(']');
    }
    return sb.toString();
  }

  public void addColumnMetrics(ColumnMetrics metrics) {
    columnMetrics.add(metrics);
  }

  public void doStatistics() {
    for (int i = 0; i < columnMetrics.size(); i++) {
      dataRead += columnMetrics.get(i).dataRead;
    }
  }
}
