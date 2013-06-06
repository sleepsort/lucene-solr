package org.apache.lucene.codecs;

import java.io.IOException;


import org.apache.lucene.index.TermState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;

public class TermMetaData {
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
  // outputs in a FST, e.g. a TermProtoData shared by two arcs might
  // get a 'skewed' output, which is not possible to be compared with others
  // Therefore during building phase, we have to iterate each long value 
  // to see whether the 'comparable' property still holds.
  //
  // NOTE: only use signed part of long value, which is 63 bits
  //
  public final LongsRef base;
  public final BytesRef extend;

  public TermMetaData() {
    this.base = null;
    this.extend = null;
  }

  public TermMetaData(LongsRef l, BytesRef b) {
    this.base = l;
    this.extend = b;
  }

  public String toString() {
    return base.toString() + " " + extend.toString();
  }
  
  public void write(DataOutput out, TermValues values) throws IOException {
    throw new IllegalStateException("not implemented");
  }
  public void read(DataInput in, TermValues values) throws IOException {
    throw new IllegalStateException("not implemented");
  }

}
