package org.apache.lucene.codecs;

import java.io.IOException;


import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;

public class TermProtoData implements Comparable<TermProtoData> {
  // TermProtoData consists of two parts:
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

  public TermProtoData() {
    this.base = null;
    this.extend = null;
  }

  public TermProtoData(LongsRef l, BytesRef b) {
    this.base = l;
    this.extend = b;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TermProtoData) {
      return this.base.equals(((TermProtoData)other).base);
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(TermProtoData other) {
    // nocommit: just lazy to write the full codes:
    // to use this, we should make sure it is 
    // really comparable first
    return this.base.compareTo(other.base);
  }

  @Override
  public String toString() {
    return base.toString() + " " + extend.toString();
  }

}
