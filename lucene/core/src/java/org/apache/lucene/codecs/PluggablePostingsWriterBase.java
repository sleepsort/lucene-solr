package org.apache.lucene.codecs;

import java.io.IOException;
import java.io.Closeable;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.index.FieldInfo;

public abstract class PluggablePostingsWriterBase extends PostingsConsumer implements Closeable {

  protected PluggablePostingsWriterBase() {
  }

  public abstract void start(IndexOutput termsOut) throws IOException;

  public abstract void startTerm() throws IOException;

  public abstract void flushTermsBlock(int start, int count) throws IOException;

  public abstract void finishTerm(TermStats stats) throws IOException;

  public abstract void setField(FieldInfo fieldInfo);

  @Override
  public abstract void close() throws IOException;
}
