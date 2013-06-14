package org.apache.lucene.codecs;

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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.codecs.temp.TempTermState;

public abstract class TermMetaData implements Cloneable {

  /* no arg means the instance will be always 'less than' any other instance */
  public TermMetaData() {
  }

  public TermMetaData clone() {
    try {
      return (TermMetaData)super.clone();
    } catch (CloneNotSupportedException cnse) {
      throw new RuntimeException(cnse);
    }
  }
  public abstract void copyFrom(TermMetaData other);

  /* return (this - smaller), if possible */
  public abstract TermMetaData subtract(TermMetaData inc);

  /* return (this + inc), if possible */
  public abstract TermMetaData add(TermMetaData inc);

  public abstract void write(DataOutput out, FieldInfo info, TempTermState state) throws IOException;

  public abstract void read(DataInput out, FieldInfo info, TempTermState state) throws IOException;

  public String toString() {
    return "TermMetaData";
  }
}
