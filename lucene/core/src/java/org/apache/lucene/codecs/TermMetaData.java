package org.apache.lucene.codecs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.index.TermState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;

public class TermMetaData implements Cloneable {
  // It consists of two parts:
  //
  // The base part: 
  //   long array, in which every value increases 
  //   monotonically at the same time.
  //
  // The extend part:
  //   byte array, in which non-monotonical values
  //   are stored/encoded.
  //
  // NOTE: For raw output, 
  // it is always assumed that, when we have
  //   this.base[i] < another.base[i],
  // for all j in [base.offset, base.end)
  //   this.base[j] <= another.base[j]
  // with this property, we might have better compression 
  // for base part.
  //
  // However, this property is not guraranteed for all intermediate
  // outputs in a FST, e.g. a TermMetaData shared by two arcs might
  // get a 'skewed' output, which is not possible to be compared with others
  // Therefore during building phase, we have to iterate each long value 
  // to see whether the 'comparable' property still holds.
  //
  // NOTE: only use signed part of long value, which is 63 bits
  //
  protected LongsRef base;
  protected BytesRef extend;

  protected ByteBuffer buffer;

  public TermMetaData() {
    this.base = null;
    this.extend = null;
    this.buffer = null;
  }

  public TermMetaData(LongsRef l, BytesRef b) {
    this.base = l;
    this.extend = b;
    this.buffer = ByteBuffer.wrap(extend.bytes, extend.offset, extend.length);
  }
  public TermMetaData(int baseLength, int extendLength) {
    if (baseLength > 0) {
      this.base = new LongsRef(new long[baseLength], 0, baseLength);
    } else {
      this.base = null;
    }
    if (extendLength > 0) {
      this.extend = new BytesRef(new byte[extendLength]);
      this.buffer = ByteBuffer.wrap(extend.bytes, extend.offset, extend.length);
    } else {
      this.extend = null;
    }
  }

  public TermMetaData clone() {
    try {
      return (TermMetaData)super.clone();
    } catch (CloneNotSupportedException cnse) {
      // should not happen
      throw new RuntimeException(cnse);
    }
  } 
  
  public void copyFrom(TermMetaData other) { // nocommit: no deepcopy!
    if (other.base != null) {
      this.base = LongsRef.deepCopyOf(other.base);
    }
    if (other.extend != null) {
      this.extend = BytesRef.deepCopyOf(other.extend);
      this.buffer = ByteBuffer.wrap(extend.bytes, extend.offset, extend.length);
    }
  }

  public String toString() {
    return "TermMetaData";
  }
  
  public void write(DataOutput out, TermState state) throws IOException {
    throw new IllegalStateException("not implemented");
  }
  public void read(DataInput in, TermState state) throws IOException {
    throw new IllegalStateException("not implemented");
  }

}
