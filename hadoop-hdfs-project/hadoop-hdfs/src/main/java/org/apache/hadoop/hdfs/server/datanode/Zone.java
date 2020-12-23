/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;


public final class Zone implements Serializable {
  private static final long serialVersionUID = 1729L;
  private File file;

  /**
   * Zonefs allows data write to seq files only with data size multiple of
   * physical sector size of the zoned block device. On the other hand, HDFS
   * blocks can be any size depending on the file size. This class keeps rest
   * of the remainder which does not fit the physical sector size.
   */
  private byte sectorRemainder[];

  /**
   * The offset bytes of the sectorRemainder from the zone start.
   */
  private long sectorRemainderOffset;

  Zone(File file) {
    this.file = file;
    sectorRemainder = new byte[0]; // Set zero length object for serialization
    sectorRemainderOffset = 0;
  }

  public String toString() {
    return "zonefs file: " + file.getAbsolutePath() + ": sector remainder at " +
      sectorRemainderOffset + "+" + sectorRemainder.length;
  }

  public File getFile() {
    return file;
  }

  public long length() {
    return file.length() + sectorRemainder.length;
  }

  public void checkFileLength() throws IOException {
    if (sectorRemainder.length > 0 && file.length() != sectorRemainderOffset)
      throw new IOException("Inconsistent zonefs file size and saved offset: "
                            + file.length() + " != " + sectorRemainderOffset);
  }

  public int sectorRemainderLen() {
    return sectorRemainder.length;
  }

  public void setSectorRemainder(byte buf[], int boff, int len, long zoff) {
    sectorRemainder = new byte[len];
    System.arraycopy(buf, boff, sectorRemainder, 0, len);
    sectorRemainderOffset = zoff;
  }

  public int copySectorRemainder(byte buf[]) {
    int len = 0;
    System.arraycopy(sectorRemainder, 0, buf, 0, sectorRemainder.length);
    len = sectorRemainder.length;
    return len;
  }

  public int copySectorRemainder(byte buf[], int bufOff,
                                 int remainderOff, int len) {
    System.arraycopy(sectorRemainder, remainderOff, buf, bufOff, len);
    return len;
  }

  public void reset() throws IOException {
    sectorRemainderOffset = 0;
    if (sectorRemainder.length > 0)
      sectorRemainder = new byte[0];
    if (file.length() == 0)
      return;
    try (FileChannel fc = new FileOutputStream(file, true).getChannel()) {
      fc.truncate(0);
    }
  }
}
