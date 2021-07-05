package com.zhu.utils;

import com.zhu.serde.FastSerdeHelper;
import com.zhu.storage.write.ds.RowBlockDS;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class SimpleBitSet {
  private long[] words;
  private int numWords;

  public SimpleBitSet() {
    this.words = new long[bit2words(RowBlockDS.MaxRecords)];
    this.numWords = words.length;
  }

  //将byte的数量化为long代表的数量，这边用1008个long来表示64512个byte（1008 = 64512/8/8）
  private int bit2words(int numBits) {
    return ((numBits - 1) >> 6) + 1;
  }

  /**
   * words能存储的数据量
   * @return
   */
  public int capacity() {
    return numWords * 64;
  }

  public void setUntil(int bitIndex) {
    int wordIndex = bitIndex >> 6; // divide by 64
    int i = 0;
    while(i < wordIndex) {
      words[i] = -1;
      i += 1;
    }
    if (wordIndex < words.length) {
      // Set the remaining bits (note that the mask could still be zero)
      long mask = ~(-1L << (bitIndex & 0x3f));
      words[wordIndex] |= mask;
    }
  }

  /**
   * Sets the bit at the specified index to true.
   * @param index the bit index
   */
  public void set(int index) {
    long bitmask = 1L << (index & 0x3f);  // mod 64 and shift
    words[index >> 6] |= bitmask;         // div by 64 and mask
  }

  public void unset(int index) {
    long bitmask = 1L << (index & 0x3f);  // mod 64 and shift
    words[index >> 6] &= ~bitmask;        // div by 64 and mask
  }

  /**
   * Return the value of the bit with the specified index. The value is true if the bit with
   * the index is currently set in this BitSet; otherwise, the result is false.
   *
   * @param index the bit index
   * @return the value of the bit with the specified index
   */
  public boolean get(int index) {
    long bitmask = 1L << (index & 0x3f);   // mod 64 and shift
    return (words[index >> 6] & bitmask) != 0;  // div by 64 and mask
  }


  /** Return the number of bits set to true in this BitSet. */
  public int cardinality() {
    int sum = 0;
    int i = 0;
    while (i < numWords) {
      sum += java.lang.Long.bitCount(words[i]);
      i += 1;
    }
    return sum;
  }

  /**
   * Clear all set bits.
   */
  public void clear() {
    int i = 0;
    while (i < numWords) {
      words[i] = 0L;
      i += 1;
    }
  }

  /**
   * Returns the index of the first bit that is set to true that occurs on or after the
   * specified starting index. If no such bit exists then -1 is returned.
   *
   * To iterate over the true bits in a BitSet, use the following loop:
   *
   *  for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
   *    // operate on index i here
   *  }
   *
   * @param fromIndex the index to start checking from (inclusive)
   * @return the index of the next set bit, or -1 if there is no such bit
   */
  public int nextSetBit(int fromIndex) {
    int wordIndex = fromIndex >> 6;
    if (wordIndex >= numWords) {
      return -1;
    }

    // Try to find the next set bit in the current word
    int subIndex = fromIndex & 0x3f;
    long word = words[wordIndex] >> subIndex;
    if (word != 0) {
      return (wordIndex << 6) + subIndex + java.lang.Long.numberOfTrailingZeros(word);
    }

    // Find the next set bit in the rest of the words
    wordIndex += 1;
    while (wordIndex < numWords) {
      word = words[wordIndex];
      if (word != 0) {
        return (wordIndex << 6) + java.lang.Long.numberOfTrailingZeros(word);
      }
      wordIndex += 1;
    }
    return -1;
  }

  public short[] simpleBitSet2ShortArray() {
    FastShortArray array = new FastShortArray();
    int i = 0;
    int length = capacity();
    while (i < length) {
      int j = nextSetBit(i);
      if (j >= 0) {
        array.add((short)j);
        i = j + 1;
      } else {
        break;
      }
    }
    return array.getArray();
  }

  public void spill(OutputStream out) throws IOException {
    byte[] wordBytes = FastSerdeHelper.serLongBatch(words, words.length);
    out.write(wordBytes);
  }

  public void load(ByteBuffer buffer) {
    words = FastSerdeHelper.deLongBatch(buffer, buffer.position(), numWords);
    buffer.position(buffer.position() + (numWords << FastSerdeHelper.LongShiftSize));
  }
}
