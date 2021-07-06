package com.zhu.storage.write.ds;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.serde.ZdbType;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

import static org.junit.Assert.*;

public class RowBlockDSTest {

  private Random random = new Random();

  @Test
  public void getKey() {
    RowBlockDS rowBlockDS = new RowBlockDS();
    int key = rowBlockDS.getKey(1, 1);
    System.out.println(key);
  }

  @Test
  public void testPutAndGet() throws IOException {
    int[] columnBlocksType = new int[2];
    int[] columnDataType = new int[2];
    int[] columnDataLength = new int[2];
    columnBlocksType[0] = 1;
    columnBlocksType[1] = 1;
    columnDataType[0] = ZdbType.INT;
    columnDataType[1] = ZdbType.INT;
    columnDataLength[0] = 4;
    columnDataLength[1] = 4;
    RowBlockDS ds = new RowBlockDS(columnBlocksType, columnDataType, columnDataLength);
    ds.initBuilder(2);
    ds.setBlockNum(0);
    genRows(ds);
  }

  private void genRows(RowBlockDS ds) throws IOException {
    int[] columnDataTypes = ds.getColumnDataType();
    int columnNum = columnDataTypes.length;
    int rowNum = 100 + random.nextInt(10);
    for (int r = 0; r < rowNum; r++) {
      byte[][] row = new byte[columnNum][];
      for (int i = 0; i < columnNum; i++) {
        switch (columnDataTypes[i]) {
          case ZdbType.BYTE:
            byte b = genByte();
            if (!shouldBeNull()) {
              row[i] = FastSerdeHelper.serByteBatch(new byte[]{b}, 1);
            } else {
              row[i] = null;
            }
            break;
          case ZdbType.BOOLEAN:
            boolean bo = genBoolean();
            if (!shouldBeNull()) {
              row[i] = FastSerdeHelper.serBooleanBatch(new boolean[]{bo}, 1);
            } else {
              row[i] = null;
            }
            break;
          case ZdbType.SHORT:
            short s = genShort();
            if (!shouldBeNull()) {
              row[i] = FastSerdeHelper.serShortBatch(new short[]{s}, 1);
            } else {
              row[i] = null;
            }
            break;
          case ZdbType.INT:
            int in = genInt();
            if (!shouldBeNull()) {
              row[i] = FastSerdeHelper.serIntBatch(new int[]{in}, 1);
            } else {
              row[i] = null;
            }
            break;
          case ZdbType.FLOAT:
            float f = genFloat();
            if (!shouldBeNull()) {
              row[i] = FastSerdeHelper.serFloatBatch(new float[]{f}, 1);
            } else {
              row[i] = null;
            }
            break;
          case ZdbType.LONG:
            long l = genLong();
            if (!shouldBeNull()) {
              row[i] = FastSerdeHelper.serLongBatch(new long[]{l}, 1);
            } else {
              row[i] = null;
            }
            break;
          case ZdbType.DOUBLE:
            double d = genDouble();
            if (!shouldBeNull()) {
              row[i] = FastSerdeHelper.serDoubleBatch(new double[]{d}, 1);
            } else {
              row[i] = null;
            }
            break;
          case ZdbType.OTHER:
            BigDecimal bigDecimal = genBigDecimal();
            byte[] decimal = serializeDecimal(bigDecimal);
            if (!shouldBeNull()) {
              row[i] = FastSerdeHelper.serOther(decimal);
            } else {
              row[i] = null;
            }
            break;
        }
      }
      ds.put(row);
    }
  }

  private boolean shouldBeNull() {
    return random.nextInt(3) == 0;
  }

  private byte genByte() {
    return (byte)random.nextInt(Byte.MAX_VALUE);
  }

  private boolean genBoolean() {
    return random.nextBoolean();
  }

  private int genInt() {
    return random.nextInt(Integer.MAX_VALUE);
  }

  private long genLong() {
    return random.nextLong();
  }

  private float genFloat() {
    return random.nextFloat();
  }

  private double genDouble() {
    return random.nextDouble();
  }

  private short genShort() {
    return (short)random.nextInt(Short.MAX_VALUE);
  }

  public BigDecimal genBigDecimal() {
    BigDecimal randFromDouble = new BigDecimal(random.nextDouble());
    return randFromDouble.setScale(random.nextInt(6),BigDecimal.ROUND_HALF_UP);
  }

  private byte[] serializeDecimal(BigDecimal bigDecimal) {
    BigInteger theInt = bigDecimal.unscaledValue();
    int scale = bigDecimal.scale();
    byte[] arr = theInt.toByteArray();
    byte[] decimal = new byte[arr.length + 2];
    decimal[0] = (byte)scale;
    decimal[1] = (byte)arr.length;
    System.arraycopy(arr, 0, decimal, 2, arr.length);
    return decimal;
  }
}
