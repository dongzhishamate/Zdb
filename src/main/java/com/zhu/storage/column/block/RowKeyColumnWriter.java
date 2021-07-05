package com.zhu.storage.column.block;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.storage.write.ds.metrics.ColumnMetrics;
import com.zhu.utils.OptimizedBitSet;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class RowKeyColumnWriter {
  private int row_idx = 0;
  private ArrayList<byte[]> offsets;
  private ArrayList<byte[]> rowSetIds;

  private OptimizedBitSet deleteMark;

  public RowKeyColumnWriter() {
    offsets = new ArrayList<byte[]>();
    rowSetIds = new ArrayList<byte[]>();
    deleteMark = new OptimizedBitSet();
  }

  public void put(byte[] offset) {
    put(offset, false);
  }

  public void put(byte[] offset, boolean isDelete) {
    offsets.add(offset);
    if (isDelete) {
      deleteMark.add(row_idx);
    }
    row_idx++;
  }

  public void put(byte[] rowSetId, byte[] offset) {
    put(rowSetId, offset, false);
  }

  public void put(byte[] rowSetId, byte[] offset, boolean isDelete) {
    offsets.add(offset);
    rowSetIds.add(rowSetId);
    if (isDelete) {
      deleteMark.add(row_idx);
    }
    row_idx++;
  }

  public void reuse() {
    offsets.clear();
    rowSetIds.clear();
    deleteMark.clear();
    row_idx = 0;
  }

  public void spill(DataOutputStream dataOutputStream) throws IOException {
    FastByteArrayOutputStream rowKeyColumnStream = new FastByteArrayOutputStream();
    DataOutputStream internalOutputStream = new DataOutputStream(rowKeyColumnStream);
    rowKeyColumnStream.reset();
    //rowKeyVersion.ordinal()
    rowKeyColumnStream.write(FastSerdeHelper.serInt(2));
    rowKeyColumnStream.write(compressOffsetColumn());
    rowKeyColumnStream.write(compressRowSetIdColumn());
    deleteMark.spill(internalOutputStream, new ColumnMetrics());
    rowKeyColumnStream.flush();
    dataOutputStream.write(FastSerdeHelper.serInt(rowKeyColumnStream.length));
    dataOutputStream.write(rowKeyColumnStream.array, 0, rowKeyColumnStream.length);
    rowKeyColumnStream.close();
  }

  public byte[] compressRowSetIdColumn() {
    ByteBuffer buffer = ByteBuffer.allocate((rowSetIds.size() << FastSerdeHelper.LongShiftSize) + 4);
    buffer.putInt(rowSetIds.size());
    for (byte[] rowSetId: rowSetIds) {
      buffer.put(rowSetId);
    }
    return buffer.array();
  }

  public byte[] compressOffsetColumn() {
    ByteBuffer buffer = ByteBuffer.allocate((offsets.size() << FastSerdeHelper.IntShiftSize) + 4);
    buffer.putInt(offsets.size());
    for (byte[] rowKey: offsets) {
      buffer.put(rowKey);
    }
    return buffer.array();
  }
}
