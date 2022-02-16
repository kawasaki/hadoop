Overall
=======

This is a fork of Hadoop 3.3.0 which implements zonefs support in HDFS as a
research project. Zonefs is a simple filsystem which allows to access zones of
zoned stroage as files. For detail of zoned stroage and zonefs, please visit
zonedstorage.io:

   https://zonedstorage.io/
   https://zonedstorage.io/docs/linux/fs#zonefs

About Hadoop and HDFS, please visit Hadoop web site and wiki:

   http://hadoop.apache.org/
   https://cwiki.apache.org/confluence/display/HADOOP/

This implementation allows to run Hadoop and HDFS applications on zonefs set up
on top of zoned-stroage. Two benchmark applications DFSIO and TeraSort were
confirmed to work on zonefs.

This implementation was developed and confirmed with Fedora 35 on Hadoop nodes.
SMR HDDs were attached to DataNodes and used for confirmation.


How to set up zonefs for Hadoop HDFS
====================================

1. Follow Hadoop user guide on Hadoop web site to set up Hadoop and HDFS nodes.
   HDFS DataNodes shall have Linux based OS which support zoned block device and
   zonefs. Fedora 35 is the recommended OS. DataNodes also require libaio
   runtime library. Install libaio package to each DataNode.

2. Follow BUILDING.txt and build this Hadoop fork, then deploy it to the Hadoop
   nodes. To build this fork, libaio.h and libaio.so are required. To build on
   Fedora, install libaio-devel package to your build environment.

3. Set up zoned-storage on DataNodes and create zonefs instances for them.
   Format and mount zonefs for all DataNodes in same manner.

4. Modify hdfs-site.xml and set up parameters below:

   - dfs.blocksize:
       Default size of the HDFS block files. Set zone size of the zoned storage
       on DataNodes.

   - dfs.data.zonefs.dir
       Determines where zonefs instances are mounted. It can list multiple mount
       points as comma separated values.

   - dfs.data.zonefs.meta.dir
       Determines where zonefs meta data is saved. Meta data keeps valid data
       size of each zone and sector ramainders.

   - dfs.data.zonefs.buffer.size
       Specifies buffers size used for each data write to ZoneFs file. Its
       default value is 0. When 0 is set, read the value of sysfs max_sectors_kb
       attribute and use it for the buffer size.

   - dfs.data.zonefs.max.io
       Defines limit of maximum IOs, or queue depth, which can be queued and
       executed in parallel. Its default value is 32.

5. Start the HDFS and confirm the DataNodes recoginize zonefs instances. In
   DataNode log, you will see ZoneFs message like this:

       INFO org.apache.hadoop.hdfs.server.datanode.ZoneFs: ZoneFs device: \
       root=/zonefs0, dev=/dev/sda, max I/O bytes=524288, align bytes=4096


Example of hdfs-site.xml
========================

This is an example of parameter settings for zonefs. In this example, zone size
of zoned-storage is 256MiB, then this number is set to dfs.blocksize. Each
DataNode has two zonefs instances mounted at /zonefs0 and /zonefs1. Zonefs
metadata is kept at /opt/hadoop/zonefs_meta directory. Also non-default values
are set to zonefs I/O buffer size and max IO limit.

  <property>
    <name>dfs.blocksize</name>
    <value>256m</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/opt/hadoop/home/data</value>
  </property>
  <property>
    <name>dfs.datanode.zonefs.dir</name>
    <value>/zonefs0,/zonefs1</value>
  </property>
  <property>
    <name>dfs.datanode.zonefs.meta.dir</name>
    <value>/opt/hadoop/zonefs_meta</value>
  </property>
  <property>
    <name>dfs.datanode.zonefs.buffer.size</name>
    <value>524288</value>
  </property>
  <property>
    <name>dfs.datanode.zonefs.max.io</name>
    <value>64</value>
  </property>


Left works
==========

1. Conventional zone support

   Zonefs has two sub-directories: "cnv" for conventional zones and "seq" for
   sequential write required zones. Current HDFS implementation supports only
   zonefs files in "seq" sub-directory. Further work is required to use zonefs
   files in "cnv" sub-directory and conventional zones.

2. Free space accounting

   Free space accounting of zonefs instances needs zonefs unique implementation.
   Current implementation lacks it then "hdfs dfs -df" command does not report
   valid numbers.

3. Two ZoneFsFileIoProvider methods not yet implemented

   ZoneFsFileIoProvider class extends FileIoProvider class and hides zonefs
   unique file operations. Still the ZoneFsFileIoProvider does not implement two
   override methods, getRandomAccessFile() and nativeCopyFileUnubuffered().
   Zonefs unique implementations of these methods are not required for HDFS data
   I/O benchmark on zonefs. For completeness, will need to revisit and implement
   the methods.
