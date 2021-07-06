package com.zhu.storage.write.ds;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.storage.column.block.RowKeyColumnBlock;
import com.zhu.storage.write.ds.metrics.ColumnMetrics;
import com.zhu.storage.write.ds.metrics.RowBlockMetrics;
import com.zhu.utils.ByteBufferInputStream;
import com.zhu.utils.BytesUtils;
import com.zhu.utils.Compression;
import com.zhu.utils.FastIntArray;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
  private int[] columnBlocksType = null;
  //对应column存储的是什么数据类型
  private int[] columnDataType = null;
  //对应的column一个数据类型在ZdbType中的标号,例如如果该行对应的数据类型是int,则在DataLength对应的额就是ZdbType中对应的序号3
  private int[] columnDataLength = null;

  /*
   * Create for reader
   */
  public FastIntArray readCols = new FastIntArray();

  protected boolean rowBlockContainsRowKeyColumn = true;
  protected RowKeyColumnBlock rowKeyColumnBlock;
  private RowBlockMetrics rbm = null;
  protected int blockNum = -1;

  private String fileName = null;

  protected byte[] tabletId;

  private int bucketId;

  public RowBlockDS() {
    this.rbm = new RowBlockMetrics();
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
    this.rbm = new RowBlockMetrics();
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
    rowKeyColumnBlock = new RowKeyColumnBlock();
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
      ColumnMetrics cm = new ColumnMetrics();
      columnBlocks[i].setColumnMetrics(cm);
      rbm.columnMetrics.add(cm);
    }
    //查询优化模块暂时不支持
//    if (cube != null) {
//      cubeDS = new CubeDS(cube);
//    }
  }

  public void put(byte[][] row) throws IOException {
    put(row, false);
  }

  public void put(byte[][] row, boolean containRowKey) throws IOException {
    put(row, containRowKey, false);
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
      columnBlocks[i].put(rowWithRowKey[i], 0, rowWithRowKey[i].length, isNull);
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

  public void spill(OutputStream out) throws IOException {
    DataOutputStream oos = new DataOutputStream(out);
    FastByteArrayOutputStream aos = new FastByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(aos);
    blockIndex = new int[colNums];
    if (rowNums != 0 && colNums != 0) {
      FastByteArrayOutputStream fba = new FastByteArrayOutputStream();
      int t = 0;
      for (ColumnBlockDS columnBlockDS : columnBlocks) {
        int start = aos.length;
        OutputStream cos;
        boolean needClose = true;
        //判断是否开启压缩
        if (columnBlockDS.isCompressed(compressCodec)) {
          cos = Compression.compressOutputStream(dos, Compression.defaultBufferHint, compressCodec);
        } else {
          cos = dos;
          needClose = false;
        }
        fba.reset();
        fba.write(columnBlockDS.getType());
        columnBlockDS.spill(fba);

        BytesUtils.writeInt(cos, fba.length);
        cos.write(fba.array, 0, fba.length);
        cos.flush();
        if (needClose) {
          cos.close();
        }
        // The length after compression, use to jump block index.
        blockIndex[t++] = aos.length - start;
      }
      rowKeyColumnBlock.spill(dos);
      int blockLength = aos.length;
      spillMetaData(oos, blockLength);
      oos.write(aos.array, 0, aos.length);
    }
  }

  private void spillMetaData(DataOutputStream dos, int blockLength) throws IOException {
    dos.writeInt(encodeContainsRowKeyColumnToRowBlockLength(blockLength, true));
    dos.writeInt(colNums);
    dos.writeInt(rowNums);
    dos.writeInt(blockSize);
//    dos.writeShort(Short.MIN_VALUE);
    dos.write(FastSerdeHelper.serIntBatch(blockIndex, colNums));
    dos.write(FastSerdeHelper.serIntBatch(columnBlocksType, colNums));
//    dos.write(FastSerdeHelper.serIntBatch(columnBlockVersion, colNums));
    dos.write(FastSerdeHelper.serIntBatch(columnDataType, colNums));
  }

  private int encodeContainsRowKeyColumnToRowBlockLength(int rowBlockLength, boolean containsRowKey) {
    if (containsRowKey)
      return 0x80000000 | rowBlockLength;
    else
      return rowBlockLength;
  }

  public void load(ByteBuffer buffer) throws IOException {
    loadMetaHeadBytes(buffer);
    loadMetaData(buffer);
    columnBlocks = new ColumnBlockDS[colNums];
    if (rowNums != 0 && colNums != 0) {
      //可以只加载需要的列，暂时还没实现
//      boolean[] localNeedColumns = rowSetReadOptions.getLocalNeedColumns();
      loadColumnBlocks(buffer, blockIndex);
      rowKeyColumnBlock = new RowKeyColumnBlock(rowBlockContainsRowKeyColumn, buffer);
      rowKeyColumnBlock.decode(buffer);
    }
  }

  private void loadColumnBlocks(ByteBuffer buffer, int[] blockIndex) throws IOException {
    for (int i = 0; i < columnBlocks.length; i++) {
      //当前columnBlock对应的大小
      int len = blockIndex[i];
      //该columnBlocks剩下还有多少byte没有被访问，被用于后面跳过这些空间
      int colLength;
      ByteBuffer cbBuffer;
      if (compressCodec == 0) {
        colLength = len - 4;
        //跳过block的长度
        buffer.position(buffer.position() + 4);
        cbBuffer = buffer;
      } else {
        //compressed
        InputStream is = Compression.compressInputStream(new ByteBufferInputStream(buffer.slice()), compressCodec);
        int contentLength = BytesUtils.readInt(is);
        byte[] b = new byte[contentLength];
        int idx = 0;
        while (idx < contentLength) {
          idx += is.read(b, idx, b.length - idx);
        }
        cbBuffer = ByteBuffer.wrap(b);
        colLength = contentLength;
        buffer.position(buffer.position() + len);
        is.close();
      }
      //获取第i列column的存储类型
      int type = getColumnType(cbBuffer, i);
      ColumnBlockDS columnBlockDS = ColumnBlockDSFactory.createCBDInstance(type, columnDataType[i], false);
      ColumnMetrics metrics = new ColumnMetrics();
      columnBlockDS.setColumnMetrics(metrics);

      columnBlockDS.load(cbBuffer, colLength - 1);
      columnBlockDS.setRowNum(rowNums);
      columnBlocks[i] = columnBlockDS;
      columnBlocks[i].setColumnIndex(i);
      columnBlocks[i].setDataType(columnDataType[i]);
      readCols.add(i);
    }
  }

  private void loadColumnBlocks1(ByteBuffer buffer, int[] blockIndex) throws IOException {
    for (int i = 0; i < columnBlocks.length; i++) {
      ByteBuffer cbBuffer;
      //跳过block的长度
      buffer.position(buffer.position() + 4);
      //获取第i列column的存储类型
      int type = columnBlocksType[i];
      buffer.position(buffer.position() + 1);
      cbBuffer = buffer;
      ColumnBlockDS columnBlockDS = ColumnBlockDSFactory.createCBDInstance(type, columnDataType[i], false);
      ColumnMetrics metrics = new ColumnMetrics();
      columnBlockDS.setColumnMetrics(metrics);

      columnBlockDS.load(cbBuffer,  0);
      columnBlockDS.setRowNum(rowNums);
      columnBlocks[i] = columnBlockDS;
      columnBlocks[i].setColumnIndex(i);
      columnBlocks[i].setDataType(columnDataType[i]);
      readCols.add(i);
    }
  }

  private int getColumnType(ByteBuffer buffer, int columnIndex) {
    buffer.position(buffer.position() + 1);
    return columnBlocksType[columnIndex];
  }

  private void loadMetaData(ByteBuffer buffer) {
    loadMetaTailBytes(buffer);
    loadBlockIndex(buffer);
    loadColumnBlockType(buffer);
//    loadColumnBlockVersion(buffer);
    loadColumnDataType(buffer);
    readCols.clear();
  }

  private void loadColumnDataType(ByteBuffer buffer) {
    columnDataType = FastSerdeHelper.deIntBatch(buffer, buffer.position(), colNums);
    buffer.position(buffer.position() + (colNums << FastSerdeHelper.IntShiftSize));
  }

  private void loadColumnBlockType(ByteBuffer buffer) {
    columnBlocksType = FastSerdeHelper.deIntBatch(buffer, buffer.position(), colNums);
    buffer.position(buffer.position() + (colNums << FastSerdeHelper.IntShiftSize));
  }

  private void loadBlockIndex(ByteBuffer buffer) {
    blockIndex = FastSerdeHelper.deIntBatch(buffer, buffer.position(), colNums);
    buffer.position(buffer.position() + (colNums << FastSerdeHelper.IntShiftSize));
  }


  private void loadMetaTailBytes(ByteBuffer buffer) {
    byte[] tailBytes = new byte[4 + 4];
    buffer.get(tailBytes);
    rowNums = BytesUtils.decodeIntFromBytesArray(tailBytes, 0);
    blockSize = BytesUtils.decodeIntFromBytesArray(tailBytes, 4);
  }

  private void loadMetaHeadBytes(ByteBuffer buffer) {
    byte[] headBytes = new byte[4 + 4];
    buffer.get(headBytes);
    rowBlockContainsRowKeyColumn = decodeContainsRowKeyFromRowBlockLength(
            BytesUtils.decodeIntFromBytesArray(headBytes, 0)
    );
    colNums = BytesUtils.decodeIntFromBytesArray(headBytes, 4);
  }

  private boolean decodeContainsRowKeyFromRowBlockLength(int rowBlockLength) {
    return (rowBlockLength >>> 31) == 1;
  }


  public int[] getColumnBlocksType() {
    return columnBlocksType;
  }

  public void setColumnBlocksType(int[] columnBlocksType) {
    this.columnBlocksType = columnBlocksType;
  }

  public int[] getColumnDataType() {
    return columnDataType;
  }

  public void setColumnDataType(int[] columnDataType) {
    this.columnDataType = columnDataType;
  }

  public int[] getColumnDataLength() {
    return columnDataLength;
  }

  public void setColumnDataLength(int[] columnDataLength) {
    this.columnDataLength = columnDataLength;
  }

  public int getBlockNum() {
    return blockNum;
  }

  public void setBlockNum(int blockNum) {
    this.blockNum = blockNum;
  }
}
