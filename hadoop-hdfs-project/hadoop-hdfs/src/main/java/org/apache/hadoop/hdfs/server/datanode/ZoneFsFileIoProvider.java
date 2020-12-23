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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.Zone;
import org.apache.hadoop.hdfs.server.datanode.ZoneFs;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.net.SocketOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileDescriptor;
import java.io.DataInputStream;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.FileVisitor;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Arrays;

import static org.apache.hadoop.hdfs.server.datanode.FileIoProvider.OPERATION.*;

/**
 * This class extends FileIoProvider to add file I/O abstraction for ZoneFs
 * files on top of abstraction by FileIoProvider.
 */
public class ZoneFsFileIoProvider extends FileIoProvider {
  public static final Logger LOG = LoggerFactory.getLogger(
      ZoneFsFileIoProvider.class);
  public static boolean asyncIoInitialized = false;
  public static int asyncIoMax = 32;
  private static int buffSize;
  private static int awInFlight = 0;
  private static long awSubmittedBytes = 0;
  private static long awSubmittedCommands = 0;
  private static long awCompletedCommands = 0;

  private List<ZoneFs> fsList;
  private File debug;

  class ZoneFsAsyncIoEventThread extends Thread {
    private boolean stop = false;

    public void run() {
      LOG.debug("Starting async I/O thread");
      while (!stop) {
        try {
          Arrays.fill(completedWrites, (short)0);
          int n = NativeIO.getZoneFsAsyncIOEvents(completedWrites);
          if (n > 0) {
            synchronized(ZoneFsFileIoProvider.class) {
              awInFlight -= n;
              awCompletedCommands += n;
              for (int i = 0; i < n; i++) {
                LOG.debug("put async IO: {} {}", i, completedWrites[i]);
                putAsyncWrite(completedWrites[i]);
              }
              ZoneFsFileIoProvider.class.notify();
            }
          }
          if (n == 0 && awInFlight == 0) {
            // No event received.
            Thread.sleep(1);
          }
          if (n > 0) {
            LOG.debug("async IO events: {} ({}) {}", awInFlight, n,
                      awCompletedCommands);
          }
        } catch (InterruptedException e) {
          LOG.error("Interrupted Exception in async IO events: " + e);
        } catch (IOException e) {
          LOG.error("IO Error in async IO events: " +  e);
        }
      }
      LOG.debug("Stopping async I/O thread");
    }

    void shutdown() {
      stop = true;
    }
  }

  private static ZoneFsAsyncIoEventThread asyncIoEventThread;

  /**
   * Each write request.
   */
  private class AsyncWrite {
    short index;
    boolean used;
    ZoneFileOutputStream out;
    FileDescriptor fd;
    long offset;
    long length;

    AsyncWrite(short index) {
      this.index = index;
      init();
    }

    private void init() {
      used = false;
      out = null;
      fd = null;
      offset = 0;
      length = 0;
    }

    public String toString() {
      return "AsyncWrite: i=" + index + ", used=" + used + ", fd=" + fd + ": " +
        offset + "+" + length;
    }
  }

  private static AsyncWrite asyncWrites[];
  private static short completedWrites[];

  /**
   * Must be called in synchronized section for ZoneFsFileIoProvider.class
   */
  private static AsyncWrite getAsyncWrite() {
    if (awInFlight >= asyncIoMax)
      return null;
    for (int i = 0; i < asyncIoMax; i++) {
      if (!asyncWrites[i].used) {
        asyncWrites[i].used = true;
        return asyncWrites[i];
      }
    }
    LOG.error("Invalid async write status (get): awInFlight={}/{}",
              awInFlight, asyncIoMax);
    throw new Error("Invalid async write status (get): awInFlight="
                    + awInFlight);
  }

  /**
   * Must be called in synchronized section for ZoneFsFileIoProvider.class
   */
  private static void putAsyncWrite(short index) {
    for (int i = 0; i < asyncIoMax; i++) {
      if (asyncWrites[i].index == index) {
        if (!asyncWrites[i].used) {
          LOG.error("Invalid async write used status (put): index={}", index);
          throw new Error("Invalid async write used status (put): index="
                          + index);
        }
        asyncWrites[i].init();
        return;
      }
    }
    LOG.error("Invalid async write status (put): index={}", index);
    throw new Error("Invalid async write status (put): index=" + index);
  }

  /**
   * @param conf  Configuration object. May be null. When null,
   *              the event handlers are no-ops.
   * @param datanode datanode that owns this FileIoProvider. Used for
   *               IO error based volume checker callback
   */
  public ZoneFsFileIoProvider(@Nullable Configuration conf,
                        final DataNode datanode) {
    super(conf, datanode);
    fsList = ZoneFs.createZoneFs(conf);

    asyncIoMax = ZoneFs.getMaxIO();
    LOG.info("ZoneFs async I/O max: {}", asyncIoMax);

    if (asyncIoEventThread == null) {
      try {
        asyncWrites = new AsyncWrite[asyncIoMax];
        for (short i = 0; i < asyncIoMax; i++)
          asyncWrites[i] = new AsyncWrite(i);
        completedWrites = new short[asyncIoMax];
        buffSize = ZoneFs.getMaxIOBytes();
        NativeIO.setupZoneFsAsyncIO(buffSize, asyncIoMax);
        LOG.info("ZoneFs async I/O buffer size: {}", buffSize);
      } catch (IOException e) {
        LOG.error("Failed to set up ZoneFs async I/O: {}", e);
        throw new Error(e);
      }

      asyncIoEventThread = new ZoneFsAsyncIoEventThread();
      asyncIoEventThread.start();
    }
  }

  public static void shutdown() {
    if (asyncIoEventThread != null) {
      try {
        NativeIO.cleanupZoneFsAsyncIO();
      } catch (IOException e) {
        LOG.error("Failed to clean up ZoneFs async I/O: {}", e);
        throw new Error(e);
      }

      asyncIoEventThread.shutdown();
      try {
        asyncIoEventThread.join();
      } catch (InterruptedException e) {
        LOG.error("Failed to clean up ZoneFs async I/O thread: {}", e);
        throw new Error(e);
      }

      asyncIoEventThread = null;
    }
  }

  /**
   * Allocate a zone file.
   */
  private File getZoneFile() throws IOException {
    for (ZoneFs fs: fsList) {
      File f = fs.allocZoneFile();
      if (f != null) {
        return f;
      }
    }
    throw new IOException("No zone file available");
  }

  /**
   * Free specified zone file.
   */
  private void putZoneFile(Zone z) throws IOException {
    for (ZoneFs fs: fsList) {
      if (fs.hasZone(z)) {
        fs.freeZoneFile(z);
        break;
      }
    }
  }

  /**
   * Get the zone object corresponding to the zone file.
   */
  private Zone getZone(File f) {
    Zone z = null;
    for (ZoneFs fs: fsList) {
      z = fs.getZone(f);
      if (z != null)
        break;
    }
    return z;
  }

  /**
   * Sync the given {@link FileOutputStream}.
   *
   * @param  volume target volume. null if unavailable.
   * @throws IOException
   */
  @Override
  public void sync(
      @Nullable FsVolumeSpi volume, FileOutputStream fos) throws IOException {
    if (!(fos instanceof ZoneFileOutputStream))
      return;
    LOG.debug("sync");
    ZoneFileOutputStream zfos = (ZoneFileOutputStream)fos;
    zfos.flushBuffer();
    quiesce(zfos);
  }

  /**
   * Call sync_file_range on the given file descriptor.
   *
   * @param  volume target volume. null if unavailable.
   */
  @Override
  public void syncFileRange(
    @Nullable FsVolumeSpi volume, FileOutputStream fos, FileDescriptor outFd,
      long offset, long numBytes, int flags) throws NativeIOException {
    LOG.debug("syncFileRange");
    ZoneFileOutputStream zfos = (ZoneFileOutputStream)fos;
    try {
      zfos.flushBuffer();
    } catch (IOException e) {
      throw new NativeIOException(e.toString(), 0);
    }
    quiesce(outFd, offset, numBytes);
  }

  /**
   * Create a file.
   * @param volume  target volume. null if unavailable.
   * @param f  File to be created.
   * @return  true if the file does not exist and was successfully created.
   *          false if the file already exists.
   * @throws IOException
   */
  @Override
  public boolean createFile(
      @Nullable FsVolumeSpi volume, File f) throws IOException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      File zoneFile = getZoneFile();
      String linkPath = f.getCanonicalPath();
      Files.createSymbolicLink(Paths.get(linkPath),
                               Paths.get(zoneFile.getCanonicalPath()));
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return true;
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * The helper method to delete the HDFS block file linked to a ZoneFs file.
   * Put the ZoneFs file back to allocation pool, and delete the symbolic link
   * to it.
   */
  private boolean deleteZoneFsFile(File file) throws IOException {
    if (ZoneFs.isZoneFile(file)) {
      LOG.debug("free up zone: {}", file);
      Zone z = getZone(file);
      try {
        putZoneFile(z);
      } catch (IOException e) {
        LOG.error("Failed to put zone: {}", z);
        return false;
      }
    }
    return file.delete();
  }

  /**
   * Delete a file.
   * @param volume  target volume. null if unavailable.
   * @param f  File to delete.
   * @return  true if the file was successfully deleted.
   */
  @Override
  public boolean delete(@Nullable FsVolumeSpi volume, File f) {
    final long begin = profilingEventHook.beforeMetadataOp(volume, DELETE);
    boolean deleted = false;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, DELETE);
      deleted = deleteZoneFsFile(f);
      profilingEventHook.afterMetadataOp(volume, DELETE, begin);
    } catch (IOException e) {
      LOG.warn("Failed to delete {}; {}", f, e);
      onFailure(volume, begin);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
    return deleted;
  }

  /**
   * Delete a file, first checking to see if it exists.
   * @param volume  target volume. null if unavailable.
   * @param f  File to delete
   * @return  true if the file was successfully deleted or if it never
   *          existed.
   */
  @Override
  public boolean deleteWithExistsCheck(@Nullable FsVolumeSpi volume, File f) {
    final long begin = profilingEventHook.beforeMetadataOp(volume, DELETE);
    boolean deleted = false;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, DELETE);
      deleted = !f.exists() || deleteZoneFsFile(f);
      profilingEventHook.afterMetadataOp(volume, DELETE, begin);
    } catch (IOException e) {
      onFailure(volume, begin);
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
    if (!deleted) {
      LOG.warn("Failed to delete file {}", f);
    }
    return deleted;
  }

  /**
   * Delete the given directory using {@link FileUtil#fullyDelete(File)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param dir  directory to be deleted.
   * @return true on success false on failure.
   */
  @Override
  public boolean fullyDelete(@Nullable FsVolumeSpi volume, File dir) {
    LOG.debug("fullyDelete: {}", dir);
    final long begin = profilingEventHook.beforeMetadataOp(volume, DELETE);
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, DELETE);
      Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
          throws IOException {
          if (deleteZoneFsFile(file.toFile())) {
            return FileVisitResult.CONTINUE;
          }
          throw new IOException("Failed to delete zonefs file: " + file);
        }
        @Override
        public FileVisitResult postVisitDirectory(Path dirPath, IOException e)
          throws IOException {
          if (e == null) {
            Files.delete(dirPath);
            return FileVisitResult.CONTINUE;
          }
          throw e;
        }
        });
      profilingEventHook.afterMetadataOp(volume, DELETE, begin);
      return true;
    } catch (IOException e) {
      LOG.warn("Failed to delete file tree: {}", dir);
      onFailure(volume, begin);
      return false;
    } catch (Exception e) {
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Transfer data from a ZoneFs file input stream to a SocketOutputStream.
   *
   * @param volume  target volume. null if unavailable.
   * @param sockOut  SocketOutputStream to write the data.
   * @param fis  FileInputStream from the ZoneFs file.
   * @param position  position within the channel where the transfer begins.
   * @param count  number of bytes to transfer.
   * @param waitTime  returns the nanoseconds spent waiting for the socket
   *                  to become writable.
   * @param transferTime  returns the nanoseconds spent transferring data.
   * @throws IOException
   */
  @Override
  public void transferToSocketFully(
      @Nullable FsVolumeSpi volume, SocketOutputStream sockOut,
      FileInputStream fis, long position, int count,
      LongWritable waitTime, LongWritable transferTime) throws IOException {
    if (!(fis instanceof ZoneFileInputStream)) {
      throw new IOException("Unexpected transferToSocketFully() invocation");
    }
    final long begin = profilingEventHook.beforeFileIo(volume, TRANSFER, count);
    try {
      ZoneFileInputStream zfis = (ZoneFileInputStream)fis;
      faultInjectorEventHook.beforeFileIo(volume, TRANSFER, count);
      zfis.transferToSocketFully(sockOut, position, count,
                                 waitTime, transferTime);
      profilingEventHook.afterFileIo(volume, TRANSFER, begin, count);
    } catch (Exception e) {
      String em = e.getMessage();
      if (!em.startsWith("Broken pipe") && !em.startsWith("Connection reset")) {
        onFailure(volume, begin);
      }
    }
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public long getPosition(FileInputStream fis) throws IOException {
    if (fis instanceof ZoneFileInputStream) {
      return ((ZoneFileInputStream)fis).offset;
    } else {
      return super.getPosition(fis);
    }
  }

  private void appendWrite(ZoneFileOutputStream out, FileDescriptor fd,
                           long fOff, byte[] buf, int off, int len)
    throws IOException{
    int retryCount = 0;
    if (fOff >= 256 * 1024 * 1024) {
      LOG.error("appendWrite: invalid offset to append: {}", fOff);
      throw new IOException("invalid offset to append");
    }
    while (true) {
      synchronized(ZoneFsFileIoProvider.class) {
        AsyncWrite wr = getAsyncWrite();

        if (wr != null) {
          awInFlight++;

          try {
            NativeIO.appendWithAlignment(wr.index, fd, fOff, buf, off, len);
          } catch (IOException ioe) {
            LOG.error("NativeIO.appendWithAlignment() failed: {}", ioe);
            putAsyncWrite(wr.index);
            awInFlight--;
            throw ioe;
          }

          wr.out = out;
          wr.fd = fd;
          wr.offset = fOff;
          wr.length = len;
          awSubmittedCommands++;
          awSubmittedBytes += len;
          break;
        }

        try {
          ZoneFsFileIoProvider.class.wait(50);
        } catch (InterruptedException e) {
          LOG.error("InterrupedException during append write wait");
        }

        if (retryCount++ > 100000) {
          throw new IOException("appendWithAlignment() failure repeated");
        }
      }
    }
    LOG.debug("append write: {} {} {} {} {}", fOff, awInFlight,
              awSubmittedCommands, awSubmittedBytes, retryCount);
  }

  /**
   * Wait until all I/Os in-flight get completed for the specified output
   * stream.
   */
  private void quiesce(ZoneFileOutputStream out) {
    synchronized(ZoneFsFileIoProvider.class) {
      LOG.debug("quiesce stream: awInFlight={}", awInFlight);
      if (awInFlight == 0)
        return;
      boolean inFlight = true;
      while (inFlight) {
        inFlight = false;
        for (int i = 0; i < asyncIoMax; i++) {
          if (asyncWrites[i].used && asyncWrites[i].out == out) {
            inFlight = true;
            break;
          }
        }
        if (inFlight) {
          LOG.debug("quiesce stream: wait");
          try {
            ZoneFsFileIoProvider.class.wait(50);
          } catch (InterruptedException e) {
            LOG.error("InterruptedException during quiesce wait");
          }
        }
      }
    }
  }

  /**
   * Wait until all I/Os in-flight get completed for the specified file
   * descriptor and range.
   */
  private void quiesce(FileDescriptor fd, long offset, long length) {
    synchronized(ZoneFsFileIoProvider.class) {
      LOG.debug("quiesce fd: awInFlight={}", awInFlight);
      if (awInFlight == 0)
        return;
      boolean inFlight = true;
      while (inFlight) {
        inFlight = false;
        for (int i = 0; i < asyncIoMax; i++) {
          if (asyncWrites[i].used && asyncWrites[i].fd == fd) {
            if (offset + length < asyncWrites[i].offset) {
              continue;
            } else if (asyncWrites[i].offset + asyncWrites[i].length < offset) {
              continue;
            }
            inFlight = true;
            break;
          }
        }
        if (inFlight) {
          LOG.debug("quiesce fd: wait");
          try {
            ZoneFsFileIoProvider.class.wait(50);
          } catch (InterruptedException e) {
            LOG.error("InterruptedException during quiesce wait");
          }
        }
      }
    }
  }

  /**
   * FileOutputStream to handle zonefs files.
   */
  private final class ZoneFileOutputStream extends FileOutputStream {
    private @Nullable final FsVolumeSpi volume;
    private File file;
    private FileDescriptor fd;
    private Zone zone;
    private long fOff;

    /**
     * The buffer to reduce number of write requests to zonefs with larger
     * write request. Accumulate data to write in this buffer.
     */
    private byte buf[];
    private int bufOff;
    private boolean closing;

    private ZoneFileOutputStream(FsVolumeSpi volume, FileDescriptor fd, File f)
      throws IOException {
      super(fd);
      this.volume = volume;
      this.fd = fd;
      file = f;
      zone = getZone(f);
      fOff = f.length();
      buf = new byte[buffSize];
      bufOff = 0;
      closing = false;

      // When the zone has sector remainder, copy them in the buffer.
      int remainderLen = zone.sectorRemainderLen();
      assert remainderLen <= buffSize : "Invalid sector remainder length";
      if (remainderLen > 0)
        bufOff = zone.copySectorRemainder(buf);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void write(int b) throws IOException {
      byte [] ba = new byte[1];
      ba[0] = (byte)b;
      write(ba, 0, 1);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void write(@Nonnull byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void write(@Nonnull byte[] b, int off, int len) throws IOException {
      LOG.debug("write: {} {}+{}+{}", file, fOff, bufOff, len);
      final long begin = profilingEventHook.beforeFileIo(volume, WRITE, len);
      try {
        faultInjectorEventHook.beforeFileIo(volume, WRITE, len);
        while (len > 0) {
          synchronized (this) {
            // Fill the buffer.
            int copyToBuf = len;
            if (len > buffSize - bufOff)
              copyToBuf = buffSize - bufOff;
            System.arraycopy(b, off, buf, bufOff, copyToBuf);
            off += copyToBuf;
            len -= copyToBuf;
            bufOff += copyToBuf;

            // When the buffer is full, write it out.
            if (bufOff == buffSize) {
              appendWrite(this, fd, fOff, buf, 0, buffSize);
              fOff += buffSize;
              bufOff = 0;
            }
          }
        }
        profilingEventHook.afterFileIo(volume, WRITE, begin, len);
      } catch(Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    private synchronized void flushBuffer()
      throws IOException {
      // When data in the buffer can be aligned to zonefs sectors, write it out.
      int align = ZoneFs.getAlignBytes();
      int alignedLen = 0;
      if (bufOff >= align) {
        alignedLen = bufOff - bufOff % align;
        appendWrite(this, fd, fOff, buf, 0, alignedLen);
        fOff += alignedLen;
        System.arraycopy(buf, alignedLen, buf, 0, bufOff - alignedLen);
        bufOff -= alignedLen;
      }

      // Save sector remainder.
      LOG.debug("flushBuffer: {} {}", bufOff, alignedLen);
      zone.setSectorRemainder(buf, 0, bufOff, fOff);
      ZoneFs.saveZoneStatus(zone);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void close() throws IOException {
      // Check if this stream is closing to avoid duplicated close.
      LOG.debug("close");
      synchronized (this) {
        if (closing)
          return;
        closing = true;
      }

      flushBuffer();

      // Close the stream as well as file descriptor of the zonefs file.
      super.close();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void flush() throws IOException {
      LOG.debug("flush");
      // Direct I/O does not need buffer flush.
    }
  }

  /**
   * FileInputStream to handle zonefs files.
   */
  private final class ZoneFileInputStream extends FileInputStream {
    private @Nullable final FsVolumeSpi volume;
    private File file;
    private long offset;
    private Zone zone;

    private ZoneFileInputStream(FsVolumeSpi volume, File f,
                                long offset, FileDescriptor fd) {
      super(fd);
      this.volume = volume;
      file = f;
      this.offset = offset;
      zone = getZone(f);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int read() throws IOException {
      byte [] ba = new byte[1];
      read(ba, 0, 1);
      return ba[0];
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int read(@Nonnull byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int read(@Nonnull byte[] b, int off, int len) throws IOException {
      final long begin = profilingEventHook.beforeFileIo(volume, READ, len);
      int sectorRemainderLen = zone.sectorRemainderLen();
      int numBytesRead = -1;

      try {
        faultInjectorEventHook.beforeFileIo(volume, READ, len);
        if (offset < file.length()) {
          if (offset + len > file.length()) {
            long newLen = file.length() - offset;
            if (newLen > Integer.MAX_VALUE) {
              throw new IOException("Invalid ZoneFs file read length:" + len);
            }
            len = (int)newLen;
          }
          numBytesRead = super.read(b, off, len);
          offset += numBytesRead;
        } else if (offset < file.length() + sectorRemainderLen) {
          long sectorRemainderOff = offset - file.length();
          if (sectorRemainderOff > Integer.MAX_VALUE) {
            throw new IOException("Invalid ZoneFs file read position:"
                                  + offset);
          }
          if (offset + len > file.length() + sectorRemainderLen) {
            long newLen = file.length() + sectorRemainderLen - offset;
            if (newLen > Integer.MAX_VALUE) {
              throw new IOException("Invalid ZoneFs file read length:" + len);
            }
            len = (int)newLen;
          }
          numBytesRead = zone.copySectorRemainder(b, off,
                                                  (int)sectorRemainderOff, len);
        }
        profilingEventHook.afterFileIo(volume, READ, begin, numBytesRead);
        return numBytesRead;
      } catch (Exception e) {
        onFailure(volume, begin);
        throw e;
      }
    }

    /**
     * Transfer data from this ZoneFileInputStrem to the given
     * SocketOutputStream. Check time elaplsed for wait and transfer and report.
     */
    private void transferToSocketFully(SocketOutputStream sockOut,
                                       long position, int count,
                                       LongWritable waitTime,
                                       LongWritable transferTime)
      throws IOException {

      // Set time values in case sockOut.transferToFully() does not set them.
      if (waitTime != null) {
        waitTime.set(0L);
      }
      if (transferTime != null) {
        transferTime.set(0L);
      }

      // If transfer from zone fs file is required, transfer them first.
      if (position < file.length()) {
        FileChannel fileCh = getChannel();
        int toTransferFromFile = count;
        if (toTransferFromFile > file.length() - position) {
          if (file.length() - position > Integer.MAX_VALUE) {
            throw new IOException("Invalid position: " + position);
          }
          toTransferFromFile = (int)(file.length() - position);
        }
        sockOut.transferToFully(fileCh, position, toTransferFromFile,
                                waitTime, transferTime);
        count -= toTransferFromFile;
        position += toTransferFromFile;
        offset = position;
      }

      // If transfer from sector left over cut end, transfer them.
      if (position >= file.length() && count > 0) {
        if (position > file.length() + zone.sectorRemainderLen()) {
          throw new IOException("Unexpected position: " + position);
        }
        long leftOffset = position - file.length();
        if (leftOffset > Integer.MAX_VALUE) {
          throw new IOException("Unexpected position: " + position);
        }
        int leftCount = count;
        if (leftOffset + leftCount > zone.sectorRemainderLen()) {
          leftCount = zone.sectorRemainderLen() - (int)leftOffset;
        }
        byte buf[] = new byte[leftCount];
        zone.copySectorRemainder(buf, 0, (int)leftOffset, leftCount);
        ByteBuffer bb = ByteBuffer.wrap(buf);
        long start = System.nanoTime();
        sockOut.waitForWritable();
        long wait = System.nanoTime();
        sockOut.write(bb);
        offset += leftCount;
        long transfer = System.nanoTime();
        if (waitTime != null) {
          waitTime.set(waitTime.get() + (wait - start));
        }
        if (transferTime != null) {
          transferTime.set(transferTime.get() + (transfer - wait));
        }
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long skip(long n) throws IOException {
      int sectorRemainderLen = zone.sectorRemainderLen();

      if (n < 0)
        throw new IOException("Negative skip length: " + n);
      if (offset + n > file.length() + sectorRemainderLen)
        throw new IOException("Skip beyond file length: " + offset + "+" + n);

      if (offset + n <= file.length())
        super.skip(n);

      offset += n;
      return n;
    }
  }

  /**
   * Create a FileInputStream using
   * {@link FileInputStream#FileInputStream(File)}.
   *
   * Wraps the created input stream to intercept read calls
   * before delegating to the wrapped stream.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @return  FileInputStream to the given file.
   * @throws  FileNotFoundException
   */
  @Override
  public FileInputStream getFileInputStream(
      @Nullable FsVolumeSpi volume, File f) throws FileNotFoundException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileInputStream fis = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      if (!ZoneFs.isZoneFile(f)) {
        fis = super.getFileInputStream(volume, f);
      } else {
        fis = new ZoneFileInputStream(volume, f, 0,
                                       new RandomAccessFile(f, "r").getFD());
      }
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fis;
    } catch (IOException e) {
      org.apache.commons.io.IOUtils.closeQuietly(fis);
      onFailure(volume, begin);
      throw new FileNotFoundException(e.getMessage());
    } catch (Exception e) {
      org.apache.commons.io.IOUtils.closeQuietly(fis);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Create a FileOutputStream using
   * {@link FileOutputStream#FileOutputStream(FileDescriptor)}.
   *
   * Wraps the created output stream to intercept write calls
   * before delegating to the wrapped stream.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @return  FileOutputStream to the given file object.
   */
  @Override
  public FileOutputStream getFileOutputStream(
      @Nullable FsVolumeSpi volume, File f) throws FileNotFoundException {
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileOutputStream fos = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      fos = new ZoneFileOutputStream(volume,
                                     NativeIO.openZoneFileToAppend(f), f);
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fos;
    } catch (IOException e) {
      org.apache.commons.io.IOUtils.closeQuietly(fos);
      onFailure(volume, begin);
      throw new FileNotFoundException(e.getMessage());
    } catch(Exception e) {
      org.apache.commons.io.IOUtils.closeQuietly(fos);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Create a FileInputStream, overriding
   * {@link FileIoProvider#getShareDeleteFileInputStream}.
   * It no longer uses {@link NativeIO#getShareDeleteFileDescriptor},
   * which is implemented for Windows.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @param offset  the offset position, measured in bytes from the
   *                beginning of the file, at which to set the file
   *                pointer.
   * @return FileOutputStream to the given file object.
   * @throws FileNotFoundException
   */
  @Override
  public FileInputStream getShareDeleteFileInputStream(
      @Nullable FsVolumeSpi volume, File f,
      long offset) throws IOException {
    if (!ZoneFs.isZoneFile(f)) {
      return super.getShareDeleteFileInputStream(volume, f, offset);
    }
    final long begin = profilingEventHook.beforeMetadataOp(volume, OPEN);
    FileInputStream fis = null;
    try {
      faultInjectorEventHook.beforeMetadataOp(volume, OPEN);
      RandomAccessFile rf = new RandomAccessFile(f, "r");
      long seekOffset = offset;
      if (seekOffset > f.length())
        seekOffset = f.length();
      rf.seek(seekOffset);
      fis = new ZoneFileInputStream(volume, f, offset, rf.getFD());
      profilingEventHook.afterMetadataOp(volume, OPEN, begin);
      return fis;
    } catch(Exception e) {
      org.apache.commons.io.IOUtils.closeQuietly(fis);
      onFailure(volume, begin);
      throw e;
    }
  }

  /**
   * Create a FileInputStream using
   * {@link FileInputStream#FileInputStream(File)} and position
   * it at the given offset.
   *
   * Wraps the created input stream to intercept read calls
   * before delegating to the wrapped stream.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @param offset  the offset position, measured in bytes from the
   *                beginning of the file, at which to set the file
   *                pointer.
   * @throws FileNotFoundException
   */
  @Override
  public FileInputStream openAndSeek(
    @Nullable FsVolumeSpi volume, File f, long offset) throws IOException {
    if (!ZoneFs.isZoneFile(f)) {
      return super.openAndSeek(volume, f, offset);
    }
    // For Linux, same implementation as getShareDeleteFileInputStream() is ok.
    return getShareDeleteFileInputStream(volume, f, offset);
  }

  /**
   * Create a RandomAccessFile using
   * {@link RandomAccessFile#RandomAccessFile(File, String)}.
   *
   * Wraps the created input stream to intercept IO calls
   * before delegating to the wrapped RandomAccessFile.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  File object.
   * @param mode  See {@link RandomAccessFile} for a description
   *              of the mode string.
   * @return RandomAccessFile representing the given file.
   * @throws FileNotFoundException
   */
  public RandomAccessFile getRandomAccessFile(
      @Nullable FsVolumeSpi volume, File f,
      String mode) throws FileNotFoundException {
    if (!ZoneFs.isZoneFile(f)) {
      return super.getRandomAccessFile(volume, f, mode);
    }
    throw new FileNotFoundException("TODO: Random access to zonefs file"
                                    + " is not yet implemented: " + f);
  }

  /**
   * Truncate the file to the specified new length.
   *
   * @param volume  target volume. null if unavailable.
   * @param f  target file to truncate
   * @param newlen  the new length the target file to have
   */
  @Override
  public void truncate(
    @Nullable FsVolumeSpi volume, File f, long newlen) throws IOException {

    Zone oldZone;
    Zone newZone;
    File newZF;
    long copied;
    int align = ZoneFs.getAlignBytes();
    byte buf[] = new byte[align];

    LOG.debug("truncate: {} {}", f, newlen);
    oldZone = getZone(f);
    if (newlen < 0 || newlen >= oldZone.length()) {
      throw new IOException("Invalid truncate length: " + newlen);
    }

    newZF = getZoneFile();
    newZone = getZone(newZF);

    try (
      FileInputStream fis = getFileInputStream(volume, f);
      DataInputStream dis = new DataInputStream(fis);
    ) {
      FileDescriptor fd = NativeIO.openZoneFileToAppend(newZF);
      // copy sector aligned data across zonefs files
      for (copied = 0; newlen - copied >= align; copied += align) {
        dis.readFully(buf);
        appendWrite(null, fd, copied, buf, 0, align);
      }
      // wait for file write completion and close the file
      quiesce(fd, 0, copied);
      FileOutputStream fos = new FileOutputStream(fd);
      fos.close();
      // copy sector remainder
      if (newlen - copied > 0) {
        int remainder = (int)(newlen - copied);
        dis.readFully(buf, 0, remainder);
        newZone.setSectorRemainder(buf, 0, remainder, copied);
      }
      ZoneFs.saveZoneStatus(newZone);
    }

    // exchange symbolic link
    Path linkPath = Paths.get(f.getAbsolutePath());
    Files.delete(linkPath);
    Files.createSymbolicLink(linkPath, Paths.get(newZF.getCanonicalPath()));

    // free up the old zone
    putZoneFile(oldZone);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long length(File f) {
    return ZoneFs.isZoneFile(f) ? ZoneFs.getZoneFileLength(f) : super.length(f);
  }

  /**
   * See {@link Storage#nativeCopyFileUnbuffered(File, File, boolean)}.
   *
   * @param volume  target volume. null if unavailable.
   * @param src  an existing file to copy, must not be {@code null}
   * @param target  the new file, must not be {@code null}
   * @param preserveFileDate  true if the file date of the copy
   *                         should be the same as the original
   * @throws IOException
   */
  @Override
  public void nativeCopyFileUnbuffered(
      @Nullable FsVolumeSpi volume, File src, File target,
      boolean preserveFileDate) throws IOException {
    throw new IOException("TODO: nativeCopyFileUnbuffered is not implemented.");
  }
}
