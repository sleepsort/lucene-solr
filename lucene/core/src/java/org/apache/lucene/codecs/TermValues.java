package org.apache.lucene.codecs;

import org.apache.lucene.index.TermsEnum; // javadocs

/**
 * Holder for per-term statistics.
 * 
 * @see TermsEnum#docFreq
 * @see TermsEnum#totalTermFreq
 */
// nocommit: actually a duplicate of TermStats!
public class TermValues {
  public final int docFreq;
  public final long totalTermFreq;
  public final int termBlockOrd;

  public TermValues(int docFreq, long totalTermFreq, int termBlockOrd) {
    this.docFreq = docFreq;
    this.totalTermFreq = totalTermFreq;
    this.termBlockOrd = termBlockOrd;
  }
}
