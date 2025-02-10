/*
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

package org.apache.paimon.disk;

import org.apache.paimon.disk.FileIOChannel.Enumerator;
import org.apache.paimon.disk.FileIOChannel.ID;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * IOManagerImpl 负责管理 Flink 任务中的 I/O 操作，提供磁盘数据溢写的功能。
 * 该类主要用于任务执行时处理大量数据的排序、哈希连接等操作，并提供文件管理功能。
 */
public class IOManagerImpl implements IOManager {

    /** 日志记录器 */
    protected static final Logger LOG = LoggerFactory.getLogger(IOManager.class);

    /** 临时目录名称的前缀 */
    private static final String DIR_NAME_PREFIX = "io";

    /** 存储临时文件的目录 */
    private final String[] tempDirs;

    /** 负责管理文件通道（FileChannel）的组件 */
    private final FileChannelManager fileChannelManager;

    // -------------------------------------------------------------------------
    //               构造函数 / 资源释放
    // -------------------------------------------------------------------------

    /**
     * 创建一个新的 IOManager 实例。
     *
     * @param tempDirs 存储临时文件的基础目录。
     */
    public IOManagerImpl(String... tempDirs) {
        this.tempDirs = tempDirs;
        this.fileChannelManager =
                new FileChannelManagerImpl(Preconditions.checkNotNull(tempDirs), DIR_NAME_PREFIX);

        // 记录 IOManager 初始化时使用的目录信息
        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "创建新的 {} 以用于任务相关数据的磁盘溢写（如连接、排序等操作）。使用的目录:\n\t{}",
                    FileChannelManager.class.getSimpleName(),
                    Arrays.stream(fileChannelManager.getPaths())
                            .map(File::getAbsolutePath)
                            .collect(Collectors.joining("\n\t")));
        }
    }

    /**
     * 关闭 IOManager，删除所有临时文件。
     */
    @Override
    public void close() throws Exception {
        fileChannelManager.close();
    }

    /**
     * 创建新的匿名文件通道（Channel），用于存储数据。
     *
     * @return 返回新创建的 Channel ID
     */
    @Override
    public ID createChannel() {
        return fileChannelManager.createChannel();
    }

    /**
     * 使用指定前缀创建新的文件通道（Channel）。
     *
     * @param prefix 指定的文件名前缀
     * @return 返回新创建的 Channel ID
     */
    @Override
    public ID createChannel(String prefix) {
        return fileChannelManager.createChannel(prefix);
    }

    /**
     * 获取 IOManager 使用的所有临时目录路径。
     *
     * @return 包含所有临时目录路径的数组
     */
    @Override
    public String[] tempDirs() {
        return tempDirs;
    }

    /**
     * 创建通道编号生成器（Enumerator）。
     *
     * @return 返回新的通道编号生成器
     */
    @Override
    public Enumerator createChannelEnumerator() {
        return fileChannelManager.createChannelEnumerator();
    }

    /**
     * 删除指定的文件通道（Channel）对应的文件。
     *
     * 如果文件仍然处于打开状态，则删除可能会失败。
     *
     * @param channel 需要删除的通道 ID
     */
    public static void deleteChannel(ID channel) {
        if (channel != null) {
            File file = channel.getPathFile();
            if (file.exists() && !file.delete()) {
                LOG.warn("IOManager 无法删除临时文件 {}", channel.getPath());
            }
        }
    }

    /**
     * 获取 IOManager 用于数据溢写的目录（File 数组）。
     *
     * @return 用于数据溢写的目录
     */
    public File[] getSpillingDirectories() {
        return fileChannelManager.getPaths();
    }

    /**
     * 获取 IOManager 用于数据溢写的目录路径（字符串数组）。
     *
     * @return 目录路径数组
     */
    public String[] getSpillingDirectoriesPaths() {
        File[] paths = fileChannelManager.getPaths();
        String[] strings = new String[paths.length];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = paths[i].getAbsolutePath();
        }
        return strings;
    }

    /**
     * 创建一个新的 BufferFileWriter 以写入数据到指定的文件通道。
     *
     * @param channelID 目标文件通道的 ID
     * @return 返回 BufferFileWriter 实例
     * @throws IOException 如果创建过程中发生 I/O 错误
     */
    @Override
    public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) throws IOException {
        return new BufferFileWriterImpl(channelID);
    }

    /**
     * 创建一个新的 BufferFileReader 以读取指定文件通道的数据。
     *
     * @param channelID 目标文件通道的 ID
     * @return 返回 BufferFileReader 实例
     * @throws IOException 如果创建过程中发生 I/O 错误
     */
    @Override
    public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID) throws IOException {
        return new BufferFileReaderImpl(channelID);
    }

    /**
     * 拆分路径字符串，使用逗号或系统路径分隔符进行分割。
     *
     * @param separatedPaths 以逗号或路径分隔符连接的路径字符串
     * @return 分割后的路径数组
     */
    public static String[] splitPaths(@Nonnull String separatedPaths) {
        return separatedPaths.length() > 0
                ? separatedPaths.split(",|" + File.pathSeparator)
                : new String[0];
    }
}

