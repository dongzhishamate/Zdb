package com.zhu.storage.write.ds;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.storage.column.block.RowKeyColumnBlock;
import com.zhu.storage.write.ds.metrics.ColumnMetrics;
import com.zhu.storage.write.ds.metrics.RowBlockMetrics;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RowBlockDS {

  protected ColumnBlockDS[] columnBlocks = null;

  public static final int BATCH_SIZE_SHIFT_POS = 10;
  public static final int BATCH_SIZE = 1 << BATCH_SIZE_SHIFT_POS;

  int colNums;
  int rowNums;
  int blockSize;

  int[] blockIndex;
  int[] columnBlockVersion;

  boolean isClosed = false;

  int compressCodec = 0;

  public static final int maxBlockSize = 16 * 1024 * 1024;
  public static final int MaxRecords = 64512;

  //对应的column用的是什么存储
  int[] columnBlocksType = null;
  //对应column存储的是什么数据类型
  int[] columnDataType = null;
  //对应的column一个数据类型在ZdbType中的标号,例如如果该行对应的数据类型是int,则在DataLength对应的额就是ZdbType中对应的序号3
  int[] columnDataLength = null;

  protected boolean rowBlockContainsRowKeyColumn = true;
  protected RowKeyColumnBlock rowKeyColumnBlock;
  private RowBlockMetrics rbm = null;
  protected int blockNum = -1;

  private String fileName = null;

  protected byte[] tabletId;

  private int bucketId;

  public RowBlockDS() {
  }

  public RowBlockDS(int[] columnBlocksType) {
    this(columnBlocksType, null, null);
  }

  public RowBlockDS(int[] columnBlocksType,
                    int[] columnDataType,
                    int[] columnDataLength) {
    this.columnBlocksType = columnBlocksType;
    this.columnDataType = columnDataType;
    this.columnDataLength = columnDataLength;
  }



  public void setBucketId(int bucketId) {
    this.bucketId = bucketId;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public void setTabletId(byte[] holodeskTabletIdForShiva) {
    this.tabletId = holodeskTabletIdForShiva;
  }

  //初始化RowBlockDS，创建列对应的columnBlocks，并将column也进行初始化
  public void initBuilder(int colNums) {
    this.colNums = colNums;
    this.rowNums = 0;
    isClosed = false;
    columnBlocks = new ColumnBlockDS[colNums];
    for (int i = 0; i < colNums; i++) {
      if (columnBlocksType == null || i >= columnBlocksType.length) {
        columnBlocks[i] = new HashDictionaryColumnBlockDS();
      } else {
        columnBlocks[i] = ColumnBlockDSFactory.createCBDInstance(columnBlocksType[i], columnDataType[i], false);
      }
      if (columnDataType != null && i < columnDataType.length) {
        columnBlocks[i].setDataType(columnDataType[i]);
      }
      columnBlocks[i].setColumnIndex(i);
      columnBlocks[i].initBuilder();
      //过滤器暂时不支持
//      columnBlockFilters[i] = newBlockFilters(columnDataType[i]);
//      for (int j = 0; j < columnBlockFilters[i].length; j++) {
//        columnBlockFilters[i][j].initFilter();
//      }
      //性能统计模块暂时不支持
      ColumnMetrics cm = new ColumnMetrics();
      columnBlocks[i].setColumnMetrics(cm);
      rbm.columnMetrics.add(cm);
    }
    //查询优化模块暂时不支持
//    if (cube != null) {
//      cubeDS = new CubeDS(cube);
//    }
  }

  public void put(byte[][] row, boolean containRowKey) {

  }

  public void put(byte[][] row, boolean containRowKey, boolean isDelete) throws IOException {
    if(isClosed) {
      throw new IOException("Can't insert into a closed block.");
    }
    byte[][] rowWithRowKey = containRowKey ? row : addRowKeyToRow(row);
    for (int i = 0; i < colNums; i++) {
      boolean isNull = false;
      if (rowWithRowKey[i] == null) {
        isNull = true;
        rowWithRowKey[i] = FastSerdeHelper.getNullValue(columnDataType[i], columnDataLength[i]);
      }
      blockSize += rowWithRowKey[i].length;
    }
    // add row key
    if (!containRowKey) {
      rowKeyColumnBlock.put(rowWithRowKey[colNums], isDelete);
    } else {
      //rowSetId + offset
      rowKeyColumnBlock.put(rowWithRowKey[colNums + 1], rowWithRowKey[colNums], isDelete);
    }
    blockSize += rowWithRowKey[colNums].length;
    rowNums ++;
  }

  public byte[][] addRowKeyToRow(byte[][] row) {
    byte[][] rowWithRowKey = new byte[colNums + 1][];
    for (int i = 0; i < colNums; i++) {
      rowWithRowKey[i] = row[i];
    }
    int rowKey = getKey(blockNum, rowNums);
    rowWithRowKey[colNums] = FastSerdeHelper.serInt(rowKey);
    return rowWithRowKey;
  }

  public int getKey(int blockId, int rowId) {
    return (blockId << 16 |rowId);
  }

//  public void load(ByteBuffer buffer) throws IOException {
//    loadMetaHeadBytes(buffer);
//    loadMetaData(buffer);
//    columnBlocks = new ColumnBlockDS[colNums];
//    columnBlockFilters = new BlockFilter[colNums][];
//    if (rowNums != 0 && colNums != 0) {
//      boolean[] localNeedColumns = rowSetReadOptions.getLocalNeedColumns();
//      loadFilterBuffer(buffer, false, intFilterIndex, localNeedColumns);
//      blockPassedFilter = blockIsPassedByFilter();
//
//      loadColumnBlocks(buffer, blockIndex, localNeedColumns);
//      rowKeyColumnBlock = new RowKeyColumnBlock(rowBlockContainsRowKeyColumn, buffer);
//      rowKeyColumnBlock.decode(buffer);
//    }
//    loadCube(buffer);
//  }
}
