package org.apache.lucene.codecs.pulsing;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.TermProtoData;
import org.apache.lucene.codecs.TermMetaData;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Concrete class that reads the current doc/freq/skip
 *  postings format 
 *  @lucene.experimental */

// TODO: -- should we switch "hasProx" higher up?  and
// create two separate docs readers, one that also reads
// prox and one that doesn't?

public class PulsingPostingsReader extends PostingsReaderBase {

  // Fallback reader for non-pulsed terms:
  final PostingsReaderBase wrappedPostingsReader;
  int maxPositions;

  public PulsingPostingsReader(PostingsReaderBase wrappedPostingsReader) {
    this.wrappedPostingsReader = wrappedPostingsReader;
  }

  @Override
  public void init(IndexInput termsIn) throws IOException {
    CodecUtil.checkHeader(termsIn, PulsingPostingsWriter.CODEC,
      PulsingPostingsWriter.VERSION_START, PulsingPostingsWriter.VERSION_START);
    maxPositions = termsIn.readVInt();
    wrappedPostingsReader.init(termsIn);
  }

  private static class PulsingTermState extends BlockTermState {
    private byte[] postings;
    private int postingsSize;                     // -1 if this term was not inlined
    private BlockTermState wrappedTermState;

    ByteArrayDataInput inlinedBytesReader;
    private byte[] inlinedBytes;

    @Override
    public PulsingTermState clone() {
      PulsingTermState clone;
      clone = (PulsingTermState) super.clone();
      if (postingsSize != -1) {
        clone.postings = new byte[postingsSize];
        System.arraycopy(postings, 0, clone.postings, 0, postingsSize);
      } else {
        assert wrappedTermState != null;
        clone.wrappedTermState = (BlockTermState) wrappedTermState.clone();
      }
      return clone;
    }

    @Override
    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
      PulsingTermState other = (PulsingTermState) _other;
      postingsSize = other.postingsSize;
      if (other.postingsSize != -1) {
        if (postings == null || postings.length < other.postingsSize) {
          postings = new byte[ArrayUtil.oversize(other.postingsSize, 1)];
        }
        System.arraycopy(other.postings, 0, postings, 0, other.postingsSize);
      } else {
        wrappedTermState.copyFrom(other.wrappedTermState);
      }

      // NOTE: we do not copy the
      // inlinedBytes/inlinedBytesReader; these are only
      // stored on the "primary" TermState.  They are
      // "transient" to cloned term states.
    }

    @Override
    public String toString() {
      if (postingsSize == -1) {
        return "PulsingTermState: not inlined: wrapped=" + wrappedTermState;
      } else {
        return "PulsingTermState: inlined size=" + postingsSize + " " + super.toString();
      }
    }
  }

  @Override
  public void readTermsBlock(IndexInput termsIn, FieldInfo fieldInfo, TermProtoData proto) throws IOException {
    //System.out.println("PR.readTermsBlock proto=" + proto);
    final BlockTermState state = proto.state;
    final PulsingMetaData meta = (PulsingMetaData) proto.meta;
    if (meta.inlinedBytes == null) {
      meta.inlinedBytes = new byte[128];
      meta.inlinedBytesReader = new ByteArrayDataInput();
    }
    int len = termsIn.readVInt();
    //System.out.println("  len=" + len + " fp=" + termsIn.getFilePointer());
    if (meta.inlinedBytes.length < len) {
      meta.inlinedBytes = new byte[ArrayUtil.oversize(len, 1)];
    }
    termsIn.readBytes(meta.inlinedBytes, 0, len);
    meta.inlinedBytesReader.reset(meta.inlinedBytes);
    state.termBlockOrd = 0;
    wrappedPostingsReader.readTermsBlock(termsIn, fieldInfo, new TermProtoData(state, meta.wrapped));
  }

  @Override
  public TermMetaData newMetaData() throws IOException {
    return new PulsingMetaData();
  }

  @Override
  public void nextTerm(FieldInfo fieldInfo, TermProtoData proto) throws IOException {
    //System.out.println("PR nextTerm");
    BlockTermState state = (PulsingTermState) proto.state;
    PulsingMetaData meta = (PulsingMetaData) proto.meta;

    // if we have positions, its total TF, otherwise its computed based on docFreq.
    long count = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 ? state.totalTermFreq : state.docFreq;
    //System.out.println("  count=" + count + " threshold=" + maxPositions);

    if (count <= maxPositions) {

      // Inlined into terms dict -- just read the byte[] blob in,
      // but don't decode it now (we only decode when a DocsEnum
      // or D&PEnum is pulled):
      meta.postingsSize = meta.inlinedBytesReader.readVInt();
      if (meta.postings == null || meta.postings.length < meta.postingsSize) {
        meta.postings = new byte[ArrayUtil.oversize(meta.postingsSize, 1)];
      }
      // TODO: sort of silly to copy from one big byte[]
      // (the blob holding all inlined terms' blobs for
      // current term block) into another byte[] (just the
      // blob for this term)...
      meta.inlinedBytesReader.readBytes(meta.postings, 0, meta.postingsSize);
      //System.out.println("  inlined bytes=" + meta.postingsSize);
    } else {
      //System.out.println("  not inlined");
      meta.postingsSize = -1;
      // TODO: should we do full copyFrom?  much heavier...?
      wrappedPostingsReader.nextTerm(fieldInfo, new TermProtoData(state, meta.wrapped));
      state.termBlockOrd++;
    }
  }

  @Override
  public DocsEnum docs(FieldInfo field, TermProtoData proto, Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
    BlockTermState state = (PulsingTermState) proto.state;
    PulsingMetaData meta = (PulsingMetaData) proto.meta;
    if (meta.postingsSize != -1) {
      PulsingDocsEnum postings;
      if (reuse instanceof PulsingDocsEnum) {
        postings = (PulsingDocsEnum) reuse;
        if (!postings.canReuse(field)) {
          postings = new PulsingDocsEnum(field);
        }
      } else {
        // the 'reuse' is actually the wrapped enum
        PulsingDocsEnum previous = (PulsingDocsEnum) getOther(reuse);
        if (previous != null && previous.canReuse(field)) {
          postings = previous;
        } else {
          postings = new PulsingDocsEnum(field);
        }
      }
      if (reuse != postings) {
        setOther(postings, reuse); // postings.other = reuse
      }
      return postings.reset(liveDocs, new TermProtoData(state, meta.wrapped));
    } else {
      if (reuse instanceof PulsingDocsEnum) {
        DocsEnum wrapped = wrappedPostingsReader.docs(field, new TermProtoData(state, meta.wrapped), liveDocs, getOther(reuse), flags);
        setOther(wrapped, reuse); // wrapped.other = reuse
        return wrapped;
      } else {
        return wrappedPostingsReader.docs(field, new TermProtoData(state, meta.wrapped), liveDocs, reuse, flags);
      }
    }
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(FieldInfo field, TermProtoData proto, Bits liveDocs, DocsAndPositionsEnum reuse,
                                               int flags) throws IOException {

    BlockTermState state = (PulsingTermState) proto.state;
    PulsingMetaData meta = (PulsingMetaData) proto.meta;

    if (meta.postingsSize != -1) {
      PulsingDocsAndPositionsEnum postings;
      if (reuse instanceof PulsingDocsAndPositionsEnum) {
        postings = (PulsingDocsAndPositionsEnum) reuse;
        if (!postings.canReuse(field)) {
          postings = new PulsingDocsAndPositionsEnum(field);
        }
      } else {
        // the 'reuse' is actually the wrapped enum
        PulsingDocsAndPositionsEnum previous = (PulsingDocsAndPositionsEnum) getOther(reuse);
        if (previous != null && previous.canReuse(field)) {
          postings = previous;
        } else {
          postings = new PulsingDocsAndPositionsEnum(field);
        }
      }
      if (reuse != postings) {
        setOther(postings, reuse); // postings.other = reuse 
      }
      return postings.reset(liveDocs, proto);
    } else {
      if (reuse instanceof PulsingDocsAndPositionsEnum) {
        DocsAndPositionsEnum wrapped = wrappedPostingsReader.docsAndPositions(field, new TermProtoData(state, meta.wrapped), liveDocs, (DocsAndPositionsEnum) getOther(reuse),
                                                                              flags);
        setOther(wrapped, reuse); // wrapped.other = reuse
        return wrapped;
      } else {
        return wrappedPostingsReader.docsAndPositions(field, new TermProtoData(state, meta.wrapped), liveDocs, reuse, flags);
      }
    }
  }

  private static class PulsingDocsEnum extends DocsEnum {
    private byte[] postingsBytes;
    private final ByteArrayDataInput postings = new ByteArrayDataInput();
    private final IndexOptions indexOptions;
    private final boolean storePayloads;
    private final boolean storeOffsets;
    private Bits liveDocs;
    private int docID = -1;
    private int accum;
    private int freq;
    private int payloadLength;
    private int cost;

    public PulsingDocsEnum(FieldInfo fieldInfo) {
      indexOptions = fieldInfo.getIndexOptions();
      storePayloads = fieldInfo.hasPayloads();
      storeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    public PulsingDocsEnum reset(Bits liveDocs, TermProtoData proto) {
      BlockTermState state = (PulsingTermState) proto.state;
      PulsingMetaData meta = (PulsingMetaData) proto.meta;
      //System.out.println("PR docsEnum state=" + state+ " docFreq=" + state.docFreq);
      assert meta.postingsSize != -1;

      // Must make a copy of meta's byte[] so that if
      // app does TermsEnum.next(), this DocsEnum is not affected
      if (postingsBytes == null) {
        postingsBytes = new byte[meta.postingsSize];
      } else if (postingsBytes.length < meta.postingsSize) {
        postingsBytes = ArrayUtil.grow(postingsBytes, meta.postingsSize);
      }
      System.arraycopy(meta.postings, 0, postingsBytes, 0, meta.postingsSize);
      postings.reset(postingsBytes, 0, meta.postingsSize);
      docID = -1;
      accum = 0;
      freq = 1;
      cost = state.docFreq;
      payloadLength = 0;
      this.liveDocs = liveDocs;
      return this;
    }

    boolean canReuse(FieldInfo fieldInfo) {
      return indexOptions == fieldInfo.getIndexOptions() && storePayloads == fieldInfo.hasPayloads();
    }

    @Override
    public int nextDoc() throws IOException {
      //System.out.println("PR nextDoc this= "+ this);
      while(true) {
        if (postings.eof()) {
          //System.out.println("PR   END");
          return docID = NO_MORE_DOCS;
        }

        final int code = postings.readVInt();
        //System.out.println("  read code=" + code);
        if (indexOptions == IndexOptions.DOCS_ONLY) {
          accum += code;
        } else {
          accum += code >>> 1;              // shift off low bit
          if ((code & 1) != 0) {          // if low bit is set
            freq = 1;                     // freq is one
          } else {
            freq = postings.readVInt();     // else read freq
          }

          if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
            // Skip positions
            if (storePayloads) {
              for(int pos=0;pos<freq;pos++) {
                final int posCode = postings.readVInt();
                if ((posCode & 1) != 0) {
                  payloadLength = postings.readVInt();
                }
                if (storeOffsets && (postings.readVInt() & 1) != 0) {
                  // new offset length
                  postings.readVInt();
                }
                if (payloadLength != 0) {
                  postings.skipBytes(payloadLength);
                }
              }
            } else {
              for(int pos=0;pos<freq;pos++) {
                // TODO: skipVInt
                postings.readVInt();
                if (storeOffsets && (postings.readVInt() & 1) != 0) {
                  // new offset length
                  postings.readVInt();
                }
              }
            }
          }
        }

        if (liveDocs == null || liveDocs.get(accum)) {
          return (docID = accum);
        }
      }
    }

    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      return docID = slowAdvance(target);
    }
    
    @Override
    public long cost() {
      return cost;
    }
  }

  private static class PulsingDocsAndPositionsEnum extends DocsAndPositionsEnum {
    private byte[] postingsBytes;
    private final ByteArrayDataInput postings = new ByteArrayDataInput();
    private final boolean storePayloads;
    private final boolean storeOffsets;
    // note: we could actually reuse across different options, if we passed this to reset()
    // and re-init'ed storeOffsets accordingly (made it non-final)
    private final IndexOptions indexOptions;

    private Bits liveDocs;
    private int docID = -1;
    private int accum;
    private int freq;
    private int posPending;
    private int position;
    private int payloadLength;
    private BytesRef payload;
    private int startOffset;
    private int offsetLength;

    private boolean payloadRetrieved;
    private int cost;

    public PulsingDocsAndPositionsEnum(FieldInfo fieldInfo) {
      indexOptions = fieldInfo.getIndexOptions();
      storePayloads = fieldInfo.hasPayloads();
      storeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    boolean canReuse(FieldInfo fieldInfo) {
      return indexOptions == fieldInfo.getIndexOptions() && storePayloads == fieldInfo.hasPayloads();
    }

    public PulsingDocsAndPositionsEnum reset(Bits liveDocs, TermProtoData proto) {
      BlockTermState state = (PulsingTermState) proto.state;
      PulsingMetaData meta = (PulsingMetaData) proto.meta;
      assert meta.postingsSize != -1;
      if (postingsBytes == null) {
        postingsBytes = new byte[meta.postingsSize];
      } else if (postingsBytes.length < meta.postingsSize) {
        postingsBytes = ArrayUtil.grow(postingsBytes, meta.postingsSize);
      }
      System.arraycopy(meta.postings, 0, postingsBytes, 0, meta.postingsSize);
      postings.reset(postingsBytes, 0, meta.postingsSize);
      this.liveDocs = liveDocs;
      payloadLength = 0;
      posPending = 0;
      docID = -1;
      accum = 0;
      cost = state.docFreq;
      startOffset = storeOffsets ? 0 : -1; // always return -1 if no offsets are stored
      offsetLength = 0;
      //System.out.println("PR d&p reset storesPayloads=" + storePayloads + " bytes=" + bytes.length + " this=" + this);
      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      //System.out.println("PR d&p nextDoc this=" + this);

      while(true) {
        //System.out.println("  cycle skip posPending=" + posPending);

        skipPositions();

        if (postings.eof()) {
          //System.out.println("PR   END");
          return docID = NO_MORE_DOCS;
        }

        final int code = postings.readVInt();
        accum += code >>> 1;            // shift off low bit
        if ((code & 1) != 0) {          // if low bit is set
          freq = 1;                     // freq is one
        } else {
          freq = postings.readVInt();     // else read freq
        }
        posPending = freq;
        startOffset = storeOffsets ? 0 : -1; // always return -1 if no offsets are stored

        if (liveDocs == null || liveDocs.get(accum)) {
          //System.out.println("  return docID=" + docID + " freq=" + freq);
          position = 0;
          return (docID = accum);
        }
      }
    }

    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      return docID = slowAdvance(target);
    }

    @Override
    public int nextPosition() throws IOException {
      //System.out.println("PR d&p nextPosition posPending=" + posPending + " vs freq=" + freq);
      
      assert posPending > 0;
      posPending--;

      if (storePayloads) {
        if (!payloadRetrieved) {
          //System.out.println("PR     skip payload=" + payloadLength);
          postings.skipBytes(payloadLength);
        }
        final int code = postings.readVInt();
        //System.out.println("PR     code=" + code);
        if ((code & 1) != 0) {
          payloadLength = postings.readVInt();
          //System.out.println("PR     new payload len=" + payloadLength);
        }
        position += code >>> 1;
        payloadRetrieved = false;
      } else {
        position += postings.readVInt();
      }
      
      if (storeOffsets) {
        int offsetCode = postings.readVInt();
        if ((offsetCode & 1) != 0) {
          // new offset length
          offsetLength = postings.readVInt();
        }
        startOffset += offsetCode >>> 1;
      }

      //System.out.println("PR d&p nextPos return pos=" + position + " this=" + this);
      return position;
    }

    @Override
    public int startOffset() {
      return startOffset;
    }

    @Override
    public int endOffset() {
      return startOffset + offsetLength;
    }

    private void skipPositions() throws IOException {
      while(posPending != 0) {
        nextPosition();
      }
      if (storePayloads && !payloadRetrieved) {
        //System.out.println("  skip payload len=" + payloadLength);
        postings.skipBytes(payloadLength);
        payloadRetrieved = true;
      }
    }

    @Override
    public BytesRef getPayload() throws IOException {
      //System.out.println("PR  getPayload payloadLength=" + payloadLength + " this=" + this);
      if (payloadRetrieved) {
        return payload;
      } else if (storePayloads && payloadLength > 0) {
        payloadRetrieved = true;
        if (payload == null) {
          payload = new BytesRef(payloadLength);
        } else {
          payload.grow(payloadLength);
        }
        postings.readBytes(payload.bytes, 0, payloadLength);
        payload.length = payloadLength;
        return payload;
      } else {
        return null;
      }
    }
    
    @Override
    public long cost() {
      return cost;
    }
  }

  @Override
  public void close() throws IOException {
    wrappedPostingsReader.close();
  }
  
  /** for a docsenum, gets the 'other' reused enum.
   * Example: Pulsing(Standard).
   * when doing a term range query you are switching back and forth
   * between Pulsing and Standard
   * 
   * The way the reuse works is that Pulsing.other = Standard and
   * Standard.other = Pulsing.
   */
  private DocsEnum getOther(DocsEnum de) {
    if (de == null) {
      return null;
    } else {
      final AttributeSource atts = de.attributes();
      return atts.addAttribute(PulsingEnumAttribute.class).enums().get(this);
    }
  }
  
  /** 
   * for a docsenum, sets the 'other' reused enum.
   * see getOther for an example.
   */
  private DocsEnum setOther(DocsEnum de, DocsEnum other) {
    final AttributeSource atts = de.attributes();
    return atts.addAttribute(PulsingEnumAttribute.class).enums().put(this, other);
  }

  /** 
   * A per-docsenum attribute that stores additional reuse information
   * so that pulsing enums can keep a reference to their wrapped enums,
   * and vice versa. this way we can always reuse.
   * 
   * @lucene.internal */
  public static interface PulsingEnumAttribute extends Attribute {
    public Map<PulsingPostingsReader,DocsEnum> enums();
  }
    
  /** 
   * Implementation of {@link PulsingEnumAttribute} for reuse of
   * wrapped postings readers underneath pulsing.
   * 
   * @lucene.internal */
  public static final class PulsingEnumAttributeImpl extends AttributeImpl implements PulsingEnumAttribute {
    // we could store 'other', but what if someone 'chained' multiple postings readers,
    // this could cause problems?
    // TODO: we should consider nuking this map and just making it so if you do this,
    // you don't reuse? and maybe pulsingPostingsReader should throw an exc if it wraps
    // another pulsing, because this is just stupid and wasteful. 
    // we still have to be careful in case someone does Pulsing(Stomping(Pulsing(...
    private final Map<PulsingPostingsReader,DocsEnum> enums = 
      new IdentityHashMap<PulsingPostingsReader,DocsEnum>();
      
    @Override
    public Map<PulsingPostingsReader,DocsEnum> enums() {
      return enums;
    }

    @Override
    public void clear() {
      // our state is per-docsenum, so this makes no sense.
      // its best not to clear, in case a wrapped enum has a per-doc attribute or something
      // and is calling clearAttributes(), so they don't nuke the reuse information!
    }

    @Override
    public void copyTo(AttributeImpl target) {
      // this makes no sense for us, because our state is per-docsenum.
      // we don't want to copy any stuff over to another docsenum ever!
    }
  }
}
