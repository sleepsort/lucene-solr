package org.apache.lucene.codecs;

import org.apache.lucene.index.TermState; 

public class TermMetaData implements Comparable<TermMetaData> {

  public int docFreq;
  public long totalTermFreq;
  public int termBlockOrd;
  
  TermProtoData proto;
  TermState state;

  public TermMetaData() {
    proto = null;
    state = null;
  }
  public TermMetaData(TermProtoData proto, TermState state) {
    this.proto = proto;
    this.state = state;
  }
  public void setProto(TermProtoData proto) {
    this.proto = proto;
  }
  public void setState(TermState state) {
    this.proto = proto;
  }
  public TermProtoData getProto() {
    return proto;
  }
  public TermState getState() {
    return state;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TermMetaData) {
      return this.proto.base.equals(((TermMetaData)other).proto.base);
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(TermMetaData other) {
    return this.proto.base.compareTo(other.proto.base);
  }

  @Override
  public String toString() {
    return "TermMetaData";
  }
}
