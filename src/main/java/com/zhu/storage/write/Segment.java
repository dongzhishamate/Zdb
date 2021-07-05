package com.zhu.storage.write;

import java.io.File;

public class Segment {

  public static int MaxSize = 256 * 1024 * 1024;
  //压缩的方式
  protected int compressCodec;
  //checkPoint文件
  protected String checkPointFile = null;
  protected int blockSize = 0;
  //写入的文件
  protected File file = null;

  /**
   * 三种模式
   */
  public enum OpenMode {
    OW,  // Overwrite
    RA,  // ReadAppend
    RO;  // ReadOnly

    public static boolean isWritableMode(OpenMode m) {
      return m != RO;
    }

    public static boolean createNewFile(OpenMode m) {
      return m == OW;
    }
  }
}
