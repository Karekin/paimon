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

package org.apache.paimon.consumer;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listOriginalVersionedFiles;
import static org.apache.paimon.utils.FileUtils.listVersionedFileStatus;

/** 管理消费者组。 */
public class ConsumerManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String CONSUMER_PREFIX = "consumer-"; // 消费者文件前缀

    private final FileIO fileIO; // 文件输入输出对象
    private final Path tablePath; // 表的根路径
    private final String branch; // 分支名称

    public ConsumerManager(FileIO fileIO, Path tablePath) {
        this(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
    }

    public ConsumerManager(FileIO fileIO, Path tablePath, String branchName) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = StringUtils.isBlank(branchName) ? DEFAULT_MAIN_BRANCH : branchName;
    }

    /**
     * 获取指定消费者ID的消费者对象。
     * @param consumerId 消费者ID
     * @return 包含消费者对象的Optional
     */
    public Optional<Consumer> consumer(String consumerId) {
        return Consumer.fromPath(fileIO, consumerPath(consumerId));
    }

    /**
     * 重置指定消费者的消费状态。
     * @param consumerId 消费者ID
     * @param consumer 消费者对象
     */
    public void resetConsumer(String consumerId, Consumer consumer) {
        try {
            fileIO.overwriteFileUtf8(consumerPath(consumerId), consumer.toJson());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 删除指定消费者。
     * @param consumerId 消费者ID
     */
    public void deleteConsumer(String consumerId) {
        fileIO.deleteQuietly(consumerPath(consumerId));
    }

    /**
     * 获取所有消费者中最小的下一个快照ID。
     * @return 包含最小快照ID的OptionalLong
     */
    public OptionalLong minNextSnapshot() {
        try {
            return listOriginalVersionedFiles(fileIO, consumerDirectory(), CONSUMER_PREFIX)
                    .map(this::consumer)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .mapToLong(Consumer::nextSnapshot)
                    .reduce(Math::min);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 清理过期的消费者。
     * @param expireDateTime 过期时间
     */
    public void expire(LocalDateTime expireDateTime) {
        try {
            listVersionedFileStatus(fileIO, consumerDirectory(), CONSUMER_PREFIX)
                    .forEach(
                            status -> {
                                LocalDateTime modificationTime =
                                        DateTimeUtils.toLocalDateTime(status.getModificationTime());
                                if (expireDateTime.isAfter(modificationTime)) {
                                    fileIO.deleteQuietly(status.getPath());
                                }
                            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** 获取所有消费者。 */
    public Map<String, Long> consumers() throws IOException {
        Map<String, Long> consumers = new HashMap<>();
        listOriginalVersionedFiles(fileIO, consumerDirectory(), CONSUMER_PREFIX)
                .forEach(
                        id -> {
                            Optional<Consumer> consumer = this.consumer(id);
                            consumer.ifPresent(value -> consumers.put(id, value.nextSnapshot()));
                        });
        return consumers;
    }

    /** 列出所有消费者ID。 */
    public List<String> listAllIds() {
        try {
            return listOriginalVersionedFiles(fileIO, consumerDirectory(), CONSUMER_PREFIX)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path consumerDirectory() {
        return new Path(branchPath(tablePath, branch) + "/consumer");
    }

    private Path consumerPath(String consumerId) {
        return new Path(
                branchPath(tablePath, branch) + "/consumer/" + CONSUMER_PREFIX + consumerId);
    }
}