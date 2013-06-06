package org.apache.lucene.codecs;

import java.io.IOException;
import java.io.Closeable;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;

public abstract class PluggablePostingsReaderBase implements Closeable {

  protected PluggablePostingsReaderBase() {
  }

  public abstract void init(IndexInput termsIn) throws IOException;

  public abstract TermMetaData newMetaData() throws IOException;

  public abstract void nextTerm(FieldInfo fieldInfo, TermProtoData meta) throws IOException;

  public abstract DocsEnum docs(FieldInfo fieldInfo, TermProtoData state, Bits skipDocs, DocsEnum reuse, int flags) throws IOException;

  public abstract DocsAndPositionsEnum docsAndPositions(FieldInfo fieldInfo, TermProtoData meta, Bits skipDocs, DocsAndPositionsEnum reuse,
                                                        int flags) throws IOException;

  @Override
  public abstract void close() throws IOException;

  public abstract void readTermsBlock(IndexInput termsIn, FieldInfo fieldInfo) throws IOException;
}
