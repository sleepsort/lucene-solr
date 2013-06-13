package org.apache.lucene.codecs.pulsing;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ArrayUtil;

final class PulsingMetaData extends TermMetaData {
  byte[] postings;
  int postingsSize;                     // -1 if this term was not inlined
  BlockTermState wrapped;

  ByteArrayDataInput inlinedBytesReader;
  byte[] inlinedBytes;

  public PulsingMetaData() {
  }

  public PulsingMetaData(TermMetaData wrapped) {
    this.wrapped = new BlockTermState();
    this.wrapped.meta = wrapped;
  }

  @Override
  public PulsingMetaData clone() {
    PulsingMetaData clone = (PulsingMetaData) super.clone();
    if (postingsSize != -1) {
      clone.postings = new byte[postingsSize];
      System.arraycopy(postings, 0, clone.postings, 0, postingsSize);
    } else {
      assert wrapped != null;
      clone.wrapped = wrapped.clone();
    }
    return clone;
  }

  @Override
  public void copyFrom(TermMetaData _other) {
    super.copyFrom(_other);
    PulsingMetaData other = (PulsingMetaData) _other;
    postingsSize = other.postingsSize;
    if (other.postingsSize != -1) {
      if (postings == null || postings.length < other.postingsSize) {
        postings = new byte[ArrayUtil.oversize(other.postingsSize, 1)];
      }
      System.arraycopy(other.postings, 0, postings, 0, other.postingsSize);
    } else {
      wrapped.copyFrom(other.wrapped);
    }

    // NOTE: we do not copy the
    // inlinedBytes/inlinedBytesReader; these are only
    // stored on the "primary" TermState.  They are
    // "transient" to cloned term states.
  }
}
