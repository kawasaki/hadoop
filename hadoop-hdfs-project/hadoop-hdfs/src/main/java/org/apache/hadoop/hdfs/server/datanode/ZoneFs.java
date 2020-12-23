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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ZONEFS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ZONEFS_META_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ZONEFS_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ZONEFS_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ZONEFS_MAX_IO_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ZONEFS_MAX_IO_DEFAULT;
import org.apache.hadoop.hdfs.server.datanode.Zone;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Comparator;

/**
 * ZoneFS is a file system which makes zoned block devices accessible
 * through files. Zoned block devices has concepte of zones and ZoneFS
 * provides files corresponding to each zone. This class manages ZoneFS
 * zone files' status and manage allocation of each zone file.
 */
public class ZoneFs {
  public static final Logger LOG = LoggerFactory.getLogger(ZoneFs.class);

  private File [] seqZoneFiles;
  private static int bufferSizeConf;
  private static int maxIO;

  public static boolean hasZoneFsConfig(Configuration conf) {
    String [] fsList = conf.getTrimmedStrings(DFS_DATANODE_ZONEFS_DIR_KEY);
    String [] meta = conf.getTrimmedStrings(DFS_DATANODE_ZONEFS_META_DIR_KEY);
    return fsList.length > 0 && meta.length == 1;
  }

  public static List<ZoneFs> createZoneFs(Configuration conf) {
    String [] meta = conf.getTrimmedStrings(DFS_DATANODE_ZONEFS_META_DIR_KEY);
    if (meta.length != 1)
      return null;
    File metaDirFile = new File(meta[0]);
    if (!metaDirFile.exists())
      metaDirFile.mkdirs();
    else if (!metaDirFile.isDirectory()) {
      LOG.error("Invalid zonefs meta directory: {}", meta[0]);
      return null;
    }
    metaRoot = metaDirFile;
    bufferSizeConf = conf.getInt(DFS_DATANODE_ZONEFS_BUFFER_SIZE_KEY,
                                 DFS_DATANODE_ZONEFS_BUFFER_SIZE_DEFAULT);
    if (bufferSizeConf < 0) {
      LOG.error("Invalid {}: {}",
                DFS_DATANODE_ZONEFS_BUFFER_SIZE_KEY, bufferSizeConf);
      return null;
    }
    maxIO = conf.getInt(DFS_DATANODE_ZONEFS_MAX_IO_KEY,
                        DFS_DATANODE_ZONEFS_MAX_IO_DEFAULT);
    if (maxIO < 1) {
      LOG.error("Invalid {}: {}", DFS_DATANODE_ZONEFS_MAX_IO_KEY, maxIO);
      return null;
    }

    String [] paths = conf.getTrimmedStrings(DFS_DATANODE_ZONEFS_DIR_KEY);
    List<ZoneFs> list = new LinkedList<ZoneFs>();
    for (String path: paths) {
      try {
        ZoneFs z = new ZoneFs(path);
        list.add(z);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    }

    return list;
  }

  public class ZoneFileComparator implements Comparator<File> {
    @Override
    public int compare(File f1, File f2) {
      return Integer.parseInt(f1.getName()) - Integer.parseInt(f2.getName());
    }
  }

  static private File metaRoot;
  static private List<ZoneFs> zoneFsList;
  static private int maxIOBytes;
  static private int alignBytes;

  private LinkedList<Zone> freeZones;
  private LinkedList<Zone> allocatedZones;
  private File metaDir;

  private int bDevSysfsInt(String devPath, String attrPath)
    throws IOException {
    String path = "/sys/block/" + devPath.substring(5) + "/" + attrPath;
    BufferedReader br = new BufferedReader(new FileReader(path));
    int n = Integer.parseInt(br.readLine());
    br.close();
    return n;
  }

  private void checkBlockDev(String rootPath) {
    String blockDevPath = null;

    // Get path to block device which mounts the ZoneFs root.
    try {
      byte [] buf = new byte[256];
      int i, count;
      Process proc = Runtime.getRuntime().exec("mount");
      BufferedReader br = new BufferedReader(
        new InputStreamReader(proc.getInputStream()));
      String s = null;
      String dev = null;
      while ((s = br.readLine()) != null) {
        if (s.indexOf(rootPath) < 0)
          continue;
        dev = s.substring(0, s.indexOf(' '));
        if (dev.startsWith("/dev/"))
          break;
      }
      br.close();
      if (dev == null)
        throw new Error("No valid block device found: " + rootPath);
      blockDevPath = dev;
    } catch (IOException ioe) {
      throw new Error("IOException in mount check: " + rootPath);
    }
    LOG.debug("ZoneFs: {}, {}", rootPath, blockDevPath);

    // Check sysfs of the block device to get max sectors of each I/O.
    if (bufferSizeConf == 0) {
      int maxSectorsKb = 0;
      try {
        maxSectorsKb = bDevSysfsInt(blockDevPath, "queue/max_sectors_kb");
      } catch (IOException ioe) {
        throw new Error("IOException in sysfs read:" + ioe.toString());
      }
      if (maxSectorsKb <= 0)
        throw new Error("Invalid maxIOBytes: " + blockDevPath);
      if (maxIOBytes == 0 || maxIOBytes > maxSectorsKb * 1024)
        maxIOBytes = maxSectorsKb * 1024;
    } else {
      maxIOBytes = bufferSizeConf;
    }

    // Check sysfs of the block device to get physical block size
    int physBlockSize = 0;
    try {
      physBlockSize = bDevSysfsInt(blockDevPath, "queue/physical_block_size");
    } catch (IOException ioe) {
      throw new Error("IOException in sysfs read:" + ioe.toString());
    }
    if (physBlockSize <= 0 || physBlockSize % 512 > 0)
      throw new Error("Invalid physical block size: " + blockDevPath);
    if (alignBytes == 0 || alignBytes > physBlockSize)
      alignBytes = physBlockSize;
    if (maxIOBytes % alignBytes > 0)
      throw new Error("Unaligned max IO bytes and align bytes: max IO bytes=" +
                      maxIOBytes + ", align bytes=" + alignBytes);

    LOG.info("ZoneFs device: root={}, dev={}, max I/O bytes={}, align bytes={}",
             rootPath, blockDevPath, maxIOBytes, alignBytes);
  }

  public ZoneFs(String rootPath) {
    LOG.debug("Construct ZoneFs: {}", rootPath);
    File seqDir = new File(rootPath + "/seq");
    if (!seqDir.exists() || !seqDir.isDirectory())
      throw new Error("Invalid ZoneFs root path: " + rootPath);

    metaDir = new File(metaRoot.getAbsolutePath()
                       + "/" + seqDir.getAbsolutePath().replace('/', '_'));
    if (!metaDir.exists())
      metaDir.mkdirs();
    else if (!metaDir.isDirectory())
      throw new Error("Invalid ZoneFs meta dir: " + metaDir);

    checkBlockDev(rootPath);

    seqZoneFiles = seqDir.listFiles();
    Arrays.sort(seqZoneFiles, new ZoneFileComparator());
    LOG.debug("# of seq zones: {}", seqZoneFiles.length);
    freeZones = new LinkedList<Zone>();
    allocatedZones = new LinkedList<Zone>();
    for (File f : seqZoneFiles) {
      File metaFile = new File(metaDir + "/" + f.getName());
      if (metaFile.exists()) {
        Zone z = loadZone(f);
        if (z != null)
          allocatedZones.add(z);
      } else if (f.length() > 0) {
        throw new Error("Unexpected zone file with data: " + f);
      } else {
        freeZones.add(new Zone(f));
      }
    }

    if (zoneFsList == null) {
      zoneFsList = new LinkedList<ZoneFs>();
      zoneFsList.add(this);
    }
  }

  public static int getMaxIOBytes() {
    return maxIOBytes;
  }

  public static int getAlignBytes() {
    return alignBytes;
  }

  public static int getMaxIO() {
    return maxIO;
  }

  public File allocZoneFile() {
    if (freeZones.isEmpty()) {
      return null;
    }
    Zone z = freeZones.get(0);
    freeZones.remove(z);
    allocatedZones.add(z);
    LOG.debug("allocated zone file: {}", z);
    return z.getFile();
  }

  private void freeZone(Zone z) {
    allocatedZones.remove(z);
    try {
      z.reset();
      saveZone(z);
    } catch (IOException e) {
      LOG.error("Failed to reset zone: {}", z);
    }
    freeZones.addFirst(z);
  }

  public void freeZoneFile(File f) throws IOException {
    for (Zone z : allocatedZones) {
      if (z.getFile().equals(f)) {
        freeZone(z);
        return;
      }
    }
    throw new IOException("ZoneFs file not found: " + f);
  }

  public void freeZoneFile(Zone zone) throws IOException {
    for (Zone z : allocatedZones) {
      if (z == zone) {
        freeZone(z);
        return;
      }
    }
    throw new IOException("ZoneFs zone not found: " + zone);
  }

  public Zone getZone(File f) {
    File realFile;
    try {
      realFile = f.toPath().toRealPath().toFile();
    } catch (IOException e) {
      LOG.error("Zone: getZone() failed: {}", f);
      return null;
    }
    LOG.debug("Zone: getZone: {}, {}", f, realFile);
    for (Zone z : allocatedZones) {
      if (z.getFile().equals(realFile)) {
        return z;
      }
    }
    return null;
  }

  public boolean hasZone(Zone zone) {
    for (Zone z : allocatedZones) {
      if (zone.equals(z))
        return true;
    }
    for (Zone z : freeZones) {
      if (zone.equals(z))
        return true;
    }
    return false;
  }

  public static boolean isZoneFile(File f) {
    try {
      String path = f.toPath().toRealPath().toString();
      return path.contains("/seq/");
    } catch (IOException e) {
      return false;
    }
  }

  public static long getZoneFileLength(File f) {
    long len = f.length();
    for (ZoneFs fs : zoneFsList) {
      Zone z = fs.getZone(f);
      if (z != null && z.sectorRemainderLen() > 0) {
        len += z.sectorRemainderLen();
        break;
      }
    }
    return len;
  }

  private void saveZone(Zone z) throws IOException {
    synchronized (z) {
      File metaFile = new File(metaDir + "/" + z.getFile().getName());
      if (metaFile.exists())
        metaFile.delete();
      try (
        FileOutputStream fos = new FileOutputStream(metaFile);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        ) {
        oos.writeObject(z);
      }
    }
  }

  private Zone loadZone(File zoneFile) {
    File metaFile = new File(metaDir + "/" + zoneFile.getName());
    Zone z = null;
    try (
      FileInputStream fis = new FileInputStream(metaFile);
      ObjectInputStream ois = new ObjectInputStream(fis);
    ) {
      z = (Zone)ois.readObject();
      z.checkFileLength();
    } catch (ClassNotFoundException | IOException e) {
      throw new Error("Failed to load zone meta file: " + zoneFile + ": " + e );
    }
    LOG.debug("load zone: {} {}", zoneFile, z);
    return z;
  }

  public static void saveZoneStatus(Zone z) throws IOException {
    for (ZoneFs fs : zoneFsList) {
      if (fs.hasZone(z)) {
        fs.saveZone(z);
        break;
      }
    }
  }
}
