package com.zhu.utils;


import com.zhu.serde.FastSerdeHelper;
import com.zhu.storage.write.ds.RowBlockDS;

public class RowIndexForOffset extends FastShortArray {

  private int[] offset;
  private int dataLength;

  public RowIndexForOffset() {

  }

  public RowIndexForOffset(int[] offset, int dataLength) {
    this.offset = offset;
    this.dataLength = dataLength;
  }

  public int[] getOffset() {
    return offset;
  }

  @Override
  public void loadInternal(int length) {
    short[] rowLengthArray = FastSerdeHelper.deShortBatch(data, data.position(), length);
    data.position(data.position() + (length << 1));
    elementData = rowLengthArray;
    capacity = size = length;
    computeOffset();
  }

  private void computeOffset() {
    offset = new int[size];
    if (size > 0) {
      offset[0] = FastShortArray.unsignedShort(elementData[0]);
      for (int i = 1; i < size; i++) {
        offset[i] = offset[i - 1] + FastShortArray.unsignedShort(elementData[i]);
      }
    }
    elementData = null;
    dataLength = offset[offset.length - 1];
  }

  public int getPieceOffset(int pieceId) {
    int pieceOffset;
    if (pieceId == 0) {
      pieceOffset = 0;
    } else {
      pieceOffset = offset[(pieceId << RowBlockDS.BATCH_SIZE_SHIFT_POS) - 1];
    }
    return pieceOffset;
  }

  public int getPieceLength(int pieceId) {
    int pieceLength;
    // last piece
    if (pieceId == (offset.length >> RowBlockDS.BATCH_SIZE_SHIFT_POS)) {
      pieceLength = dataLength - getPieceOffset(pieceId);
    } else {
      pieceLength = offset[((pieceId + 1) << RowBlockDS.BATCH_SIZE_SHIFT_POS) - 1]
              - getPieceOffset(pieceId);
    }
    return pieceLength;
  }

  public int getRelativeRowOffset(int rowNum) {
    int rowOffset;
    if (getPositionInPiece(rowNum) == 0) {
      rowOffset = 0;
    } else {
      rowOffset = offset[rowNum - 1] - getPieceOffset(getPieceId(rowNum));
    }
    return rowOffset;
  }

  public int getRowLength(int rowNum) {
    int rowLength;
    if (rowNum == 0) {
      rowLength = offset[0];
    } else {
      rowLength = offset[rowNum] - offset[rowNum - 1];
    }
    return rowLength;
  }

  public int getAbsoluteRowOffset(int rowNum) {
    int rowOffset;
    if (rowNum == 0) {
      rowOffset = 0;
    } else {
      rowOffset = offset[rowNum - 1];
    }
    return rowOffset;
  }

  private int getPositionInPiece(int rowNum) {
    return rowNum - (rowNum >> RowBlockDS.BATCH_SIZE_SHIFT_POS << RowBlockDS.BATCH_SIZE_SHIFT_POS);
  }

  private int getPieceId(int rowNum) {
    return rowNum >> RowBlockDS.BATCH_SIZE_SHIFT_POS;
  }
}
