package org.apache.lucene.codecs;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.index.TermState;

public class TermProtoOutputs extends Outputs<TermProtoData> {
  private final static TermProtoData NO_OUTPUT = new TermProtoData();
  private final static TermProtoOutputs singleton = new TermProtoOutputs();
  
  private TermProtoOutputs() {
  }

  public static TermProtoOutputs getSingleton() {
    return singleton;
  }

  @Override
  //
  // Since base blob is fixed length, when these two are 'comparable'
  // i.e. every value in LongsRef fits the same ordering, the smaller one will 
  // be the common.
  //
  // NOTE: the problem with TermProtoData is:
  // we cannot usually reuse the whole TermProtoData like IntSequenceOutputs, 
  // since the extra blob of BytesRef restricts the possibility of reusing ...
  // Therefore, the equal check in other==this doesn't actually make sense.
  //
  // NOTE: this is another case where 'doShare = false' sometimes
  // creates a smaller FST..., since BytesRef blob is seldom the same
  // between two terms (hmm, might be the same for synonym terms?), 
  // even we can fully share the LongsRef blob, we still have to create
  // an extra output for two subtracted outputs, which, increases the 
  // size of FST
  //
  // NOTE: only LongsRef are 'shared', i.e. after sharing common value,
  // the output of smaller one will be a all-zero LongsRef with BytesRef blob.
  // So we need a heuristic to compress this ...
  //
  // nocommit: Builder.add() doesn't immediatelly consumes the output data, 
  // which means, the IntsRefs after one add() should all be deeply copied 
  // instead of being reused. quite hairly to detect it here, so the caller 
  // must be careful about this.
  //
  public TermProtoData common(TermProtoData p1, TermProtoData p2) {
    if (p1 == NO_OUTPUT || p2 == NO_OUTPUT) {
      return NO_OUTPUT;
    }
    TermMetaData t1 = p1.getMetaData();
    TermMetaData t2 = p2.getMetaData();
    assert t1.base != null;
    assert t2.base != null;
    assert t1.base.length == t2.base.length;

    LongsRef base1 = t1.base, base2 = t2.base;
    long[] longs1 = base1.longs, longs2 = base2.longs;
    int pos1 = base1.offset, pos2 = base2.offset;
    int end1 = pos1 + base1.length, end2 = pos2 + base2.length;
    boolean order = true;

    if (end1 != end2) {
      return NO_OUTPUT;
    }

    while (pos1 < end1 && longs1[pos1] == longs2[pos2]) {
      pos1++;
      pos2++;
    }
    if (pos1 < end1) {
      order = (longs1[pos1] > longs2[pos2]);
      if (order) {
        // check whether base1 >= base2
        while (pos1 < end1 && longs1[pos1] >= longs2[pos2]) {
          pos1++;
          pos2++;
        }
      } else {
        // check whether base1 <= base2
        while (pos1 < end1 && longs1[pos1] <= longs2[pos2]) {
          pos1++;
          pos2++;
        }
      }
    } else {
      // equal, so we might check the extend part
      if (t1.extend != null && t1.extend.equals(t2.extend)) {
        return p1;
      } else {
        return new TermProtoData(null, null, new TermMetaData(base1, null));
      }
    }
    if (pos1 < end1) {
      // not legally comparable
      return NO_OUTPUT;
    } else if (order) {
      return new TermProtoData(null, null, new TermMetaData(base1, null));
    } else {
      return new TermProtoData(null, null, new TermMetaData(base2, null));
    }
  }

  @Override
  // nocommit: maybe we should update the javadoc about this?
  // this *actually* always assume that t2 <= t1 before calling the method
  public TermProtoData subtract(TermProtoData p1, TermProtoData p2) {
    if (p2 == NO_OUTPUT) {
      return p1;
    }
    TermMetaData t1 = p1.getMetaData();
    TermMetaData t2 = p2.getMetaData();

    assert t1.base != null;
    assert t2.base != null;

    LongsRef base1 = t1.base, base2 = t2.base;
    int pos1 = base1.offset, pos2 = base2.offset;
    int end1 = pos1 + base1.length, end2 = pos2 + base2.length;
    long[] share = new long[base1.length];
    int pos = 0;

    if (end1 != end2) {
      return NO_OUTPUT;
    }

    while (pos1 < end1) {
      share[pos] = base1.longs[pos1] - base2.longs[pos2];
      assert(share[pos] >= 0);
      pos++;
      pos1++;
      pos2++;
    }
    TermValues values = p1.values;
    TermState state = p1.state;
    TermMetaData meta = new TermMetaData(new LongsRef(share, 0, pos), null);
    return new TermProtoData(values, state, meta);
  }

  @Override
  // nocommit: need to check all-zero case?
  // so we can reuse one LongsRef
  public TermProtoData add(TermProtoData p1, TermProtoData p2) {
    if (p1 == NO_OUTPUT) {
      return p2;
    } else if (p2 == NO_OUTPUT) {
      return p1;
    }
    TermMetaData t1 = p1.getMetaData();
    TermMetaData t2 = p2.getMetaData();
    assert t1.base != null;
    assert t2.base != null;

    LongsRef base1 = t1.base, base2 = t2.base;
    int pos1 = base1.offset, pos2 = base2.offset;
    int end1 = pos1 + base1.length, end2 = pos2 + base2.length;
    long[] accum = new long[base1.length];
    int pos = 0;

    assert end1 == end2; // nocommit: we need this ?

    while (pos1 < end1) {
      accum[pos] = base1.longs[pos1] + base2.longs[pos2];
      assert(accum[pos] >= 0);
      pos++;
      pos1++;
      pos2++;
    }
    assert p1.values == null || p2.values == null;
    assert p1.state == null || p2.state == null;

    TermValues values = (p1.values == null ? p1.values : p2.values);
    TermState state = (p1.state == null ? p1.state: p2.state);
    TermMetaData meta = new TermMetaData(new LongsRef(accum, 0, pos), null);
    return new TermProtoData(values, state, meta);
  }

  @Override
  public void write(TermProtoData proto, DataOutput out) throws IOException {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public TermProtoData read(DataInput in) throws IOException {
    // nocommit: will be better if we can aquire TermState somewhere
    // to help decoding ...
    throw new IllegalStateException("not implemented");
  }

  @Override
  public TermProtoData getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(TermProtoData data) {
    return data.toString();
  }
}
