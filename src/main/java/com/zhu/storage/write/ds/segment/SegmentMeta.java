package com.zhu.storage.write.ds.segment;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

public class SegmentMeta implements Externalizable {
  private int[] colBlockType;
  private int[] colDataType;
  private int[] colDataLength;
  private int[] columnIds;
  private Set<Integer> columnNeedStatisticsIdSet;

  public SegmentMeta() {

  }

  public SegmentMeta(int[] colBlockType,
                     int[] colDataType,
                     int[] colDataLength,
                     int[] columnIds,
                     int[] columnNeedStatisticsIds) {
    this.colBlockType = colBlockType;
    this.colDataType = colDataType;
    this.colDataLength = colDataLength;
    this.columnIds = columnIds;
    columnNeedStatisticsIdSet = new HashSet<Integer>();
    if (columnNeedStatisticsIds != null) {
      for (int columnId : columnNeedStatisticsIds) {
        columnNeedStatisticsIdSet.add(columnId);
      }
    }
  }

  public int[] getColBlockType() {
    return colBlockType;
  }

  public int[] getColDataType() {
    return colDataType;
  }

  public int[] getColDataLength() {
    return colDataLength;
  }

  public int[] getColumnIds() {
    return columnIds;
  }

  public Set<Integer> getColumnNeedStatisticsIds() {
    return columnNeedStatisticsIdSet;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(colBlockType);
    out.writeObject(colDataType);
    out.writeObject(colDataLength);
    out.writeObject(columnIds);
    if (columnNeedStatisticsIdSet.isEmpty()) {
      out.writeInt(0);
    } else {
      out.writeInt(1);
      out.writeInt(columnNeedStatisticsIdSet.size());
      for (Integer id : columnNeedStatisticsIdSet) {
        out.writeInt(id);
      }
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    colBlockType = (int[]) in.readObject();
    colDataType = (int[]) in.readObject();
    colDataLength = (int[]) in.readObject();
    columnIds = (int[]) in.readObject();
    columnNeedStatisticsIdSet = new HashSet<Integer>();
    int flag = in.readInt();
    if (flag == 1) {
      int len = in.readInt();
      for (int i = 0; i < len; i++) {
        columnNeedStatisticsIdSet.add(in.readInt());
      }
    }
  }
}
