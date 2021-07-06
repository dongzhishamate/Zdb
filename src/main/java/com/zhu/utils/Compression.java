package com.zhu.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class Compression {

  public enum CompressCodec {
    NOTCOMPRESSION,
//    LZF,
//    SNAPPY,
    ZLIB,
//    LZ4,
//    ZSTD
  }

  public final static CompressCodec defaultCodec = CompressCodec.ZLIB;

  public final static int defaultBufferHint = 65535;

  public static OutputStream compressOutputStream(OutputStream os, int bufferHint) throws IOException {
    return compressOutputStream(os, bufferHint, defaultCodec.ordinal());
  }

  public static OutputStream compressOutputStream(OutputStream os, int bufferHint,
                                                  int compressCodec) throws IOException {
    switch (CompressCodec.values()[compressCodec]) {
      case NOTCOMPRESSION: {
        return os;
      }
//      case LZF : {
//      }
//      case SNAPPY: {
//      }
      case ZLIB: {
        DeflaterOutputStream sos = new DeflaterOutputStream(os);
        return sos;
      }
//      case LZ4: {
//        LZ4BlockOutputStream sos = new LZ4BlockOutputStream(os);
//        return sos;
//      }
//      case ZSTD: {
//        ZstdOutputStream sos = new ZstdOutputStream(os, 6);
//        return sos;
//      }
    }
    return new DeflaterOutputStream(os);
  }

  public static InputStream compressInputStream(InputStream is, int compressCodec) throws IOException{
    switch (CompressCodec.values()[compressCodec]) {
      case NOTCOMPRESSION: {
        return is;
      }
//      case LZF : {
//      }
//      case SNAPPY: {
//      }
      case ZLIB: {
        InflaterInputStream sos = new InflaterInputStream(is);
        return sos;
      }
//      case LZ4: {
//        LZ4BlockInputStream sos = new LZ4BlockInputStream(is);
//        return sos;
//      }
//      case ZSTD: {
//        ZstdInputStream sos = new ZstdInputStream(is);
//        return sos;
//      }
    }
    return new InflaterInputStream(is);
  }
}
