package org.apache.lucene.codecs.pulsing;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.codecs.TermProtoData;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.IOUtils;

// nocommit: not actually implemented !! This file is just written
// to pass compilation. We should refactor pulsing codec 
// separately first, before changing to this.
final class PulsingMetaData extends TermMetaData {
  byte[] postings;
  int postingsSize;                     // -1 if this term was not inlined
  TermMetaData wrapped;

  ByteArrayDataInput inlinedBytesReader;
  byte[] inlinedBytes;

  public PulsingMetaData(BlockTermState wrappedState, TermMetaData wrapped) {
    throw new IllegalStateException("not implemented");
  }
  public PulsingMetaData(FieldInfo info) {
    throw new IllegalStateException("not implemented");
  }
}
