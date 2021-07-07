package com.zhu.storage.write.ds.segment;

import com.zhu.storage.write.RandomAccessFileOutputStream;
import com.zhu.storage.write.ds.RowBlockDS;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class WritableAppendOnlySegment extends Segment {

  RandomAccessFileOutputStream raOut = new RandomAccessFileOutputStream();
  OutputStream out = null;

  public WritableAppendOnlySegment(SegmentMeta segmentMeta, File file) {
    super(segmentMeta.getColBlockType(), segmentMeta.getColDataType(), segmentMeta.getColDataLength(), file);
  }

  @Override
  protected void flush(RowBlockDS block) throws IOException {
    if (out == null) {
      raOut.setRandomAccessFile(raf);
      out = raOut;
    }
    if (this.effectiveLength <= raf.length()) {
      // Prepare overwrite data.
      raf.seek(effectiveLength);
    } else if (this.effectiveLength > raf.length()) {
      throw new IOException(String.format("Effective length greater "
                      + "than file length %s, %s.", String.valueOf(effectiveLength),
              String.valueOf(raf.length())));
    }
    block.spill(out);
    out.flush();
    this.effectiveLength = (int)raf.length();
  }

  @Override
  public void close(boolean closeCurrentBlock) throws IOException {
    if (!closeCurrentBlock) {
      throw new IOException("Can't close a segment without close block flag.");
    }
    if (currentBlock != null && !currentBlock.isClosed()) {
      if (closeCurrentBlock && !currentBlock.isClosed()) {
        currentBlock.close();
      }
      flush(currentBlock);
      currentBlock = null;
    }
    closeStream();
    //上传segment元数据信息到一个manager中，等以后有时间实现
//    if (segmentUploadHandler != null) {
//      segmentUploadHandler.uploadBase(this);
//    }
  }

  private void closeStream() throws IOException {
    if (out != null) {
      out.flush();
      out.close();
      out = null;
    }
    if (raf != null) {
      raf.close();
      raf = null;
    }
  }
}
