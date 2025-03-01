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

package org.apache.paimon.utils;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;

/**
 * Snapshot 管理器，提供与路径和快照提示相关的实用方法。
 */
public class SnapshotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

    // 快照路径前缀
    private static final String SNAPSHOT_PREFIX = "snapshot-";
    // 增量日志路径前缀
    private static final String CHANGELOG_PREFIX = "changelog-";
    // 特殊标识，表示最早的快照
    public static final String EARLIEST = "EARLIEST";
    // 特殊标识，表示最新的快照
    public static final String LATEST = "LATEST";
    // 读取提示文件的最大重试次数
    private static final int READ_HINT_RETRY_NUM = 3;
    // 读取提示文件的重试间隔（单位：秒）
    private static final int READ_HINT_RETRY_INTERVAL = 1;

    // 文件输入输出对象
    private final FileIO fileIO;
    // 表的根路径
    private final Path tablePath;
    // 分支名称
    private final String branch;

    // 构造函数，初始化无分支的管理器
    public SnapshotManager(FileIO fileIO, Path tablePath) {
        this(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
    }

    /**
     * 构造函数，指定默认分支。
     * @param fileIO 文件输入输出对象
     * @param tablePath 表的根路径
     * @param branchName 分支名称，默认为主分支
     */
    public SnapshotManager(FileIO fileIO, Path tablePath, String branchName) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        // 如果分支名称为空，则使用默认主分支
        this.branch = StringUtils.isBlank(branchName) ? DEFAULT_MAIN_BRANCH : branchName;
    }

    /**
     * 创建一个新实例，指定分支名称。
     * @param branchName 分支名称
     * @return 新的 SnapshotManager 实例
     */
    public SnapshotManager copyWithBranch(String branchName) {
        return new SnapshotManager(fileIO, tablePath, branchName);
    }

    // 获取文件输入输出对象
    public FileIO fileIO() {
        return fileIO;
    }

    // 获取表的根路径
    public Path tablePath() {
        return tablePath;
    }

    // 获取分支名称
    public String branch() {
        return branch;
    }

    /**
     * 获取指定分支的增量日志目录。
     * @return 增量日志目录路径
     */
    public Path changelogDirectory() {
        return new Path(branchPath(tablePath, branch) + "/changelog");
    }

    /**
     * 获取长期存在的增量日志路径。
     * @param snapshotId 快照 ID
     * @return 增量日志路径
     */
    public Path longLivedChangelogPath(long snapshotId) {
        return new Path(
                branchPath(tablePath, branch) + "/changelog/" + CHANGELOG_PREFIX + snapshotId);
    }

    /**
     * 获取快照路径。
     * @param snapshotId 快照 ID
     * @return 快照路径
     */
    public Path snapshotPath(long snapshotId) {
        return new Path(
                branchPath(tablePath, branch) + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    /**
     * 获取快照目录路径。
     * @return 快照目录路径
     */
    public Path snapshotDirectory() {
        return new Path(branchPath(tablePath, branch) + "/snapshot");
    }

    /**
     * 根据快照 ID 获取快照对象。
     * @param snapshotId 快照 ID
     * @return 快照对象
     */
    public Snapshot snapshot(long snapshotId) {
        Path snapshotPath = snapshotPath(snapshotId);
        return Snapshot.fromPath(fileIO, snapshotPath);
    }

    /**
     * 根据快照 ID 获取增量日志对象。
     * @param snapshotId 快照 ID
     * @return 增量日志对象
     */
    public Changelog changelog(long snapshotId) {
        Path changelogPath = longLivedChangelogPath(snapshotId);
        return Changelog.fromPath(fileIO, changelogPath);
    }

    /**
     * 根据快照 ID 获取长期存在的增量日志对象。
     * @param snapshotId 快照 ID
     * @return 长期存在的增量日志对象
     */
    public Changelog longLivedChangelog(long snapshotId) {
        return Changelog.fromPath(fileIO, longLivedChangelogPath(snapshotId));
    }

    /**
     * 检查指定快照是否存在于快照目录中。
     * @param snapshotId 快照 ID
     * @return 是否存在
     */
    public boolean snapshotExists(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "无法确定快照 #" + snapshotId + " 是否存在于路径 " + path,
                    e);
        }
    }

    /**
     * 检查指定快照的长期存在的增量日志是否存在于增量日志目录中。
     * @param snapshotId 快照 ID
     * @return 是否存在
     */
    public boolean longLivedChangelogExists(long snapshotId) {
        Path path = longLivedChangelogPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "无法确定增量日志 #" + snapshotId + " 是否存在于路径 " + path,
                    e);
        }
    }

    /**
     * 获取最新的快照对象。
     * @return 最新的快照对象，如果不存在则返回 null
     */
    public @Nullable Snapshot latestSnapshot() {
        Long snapshotId = latestSnapshotId();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    /**
     * 获取最新的快照 ID。
     * @return 最新的快照 ID，如果不存在则返回 null
     */
    public @Nullable Long latestSnapshotId() {
        try {
            return findLatest(snapshotDirectory(), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("无法找到最新的快照 ID", e);
        }
    }

    /**
     * 获取最早的快照对象。
     * @return 最早的快照对象，如果不存在则返回 null
     */
    public @Nullable Snapshot earliestSnapshot() {
        Long snapshotId = earliestSnapshotId();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    /**
     * 获取最早的快照 ID。
     * @return 最早的快照 ID，如果不存在则返回 null
     */
    public @Nullable Long earliestSnapshotId() {
        try {
            return findEarliest(snapshotDirectory(), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("无法找到最早的快照 ID", e);
        }
    }

    /**
     * 获取最早的长期存在的增量日志 ID。
     * @return 最早的增量日志 ID，如果不存在则返回 null
     */
    public @Nullable Long earliestLongLivedChangelogId() {
        try {
            return findEarliest(
                    changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("无法找到最早的增量日志 ID", e);
        }
    }

    /**
     * 获取最新的长期存在的增量日志 ID。
     * @return 最新的增量日志 ID，如果不存在则返回 null
     */
    public @Nullable Long latestLongLivedChangelogId() {
        try {
            return findLatest(changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("无法找到最新的增量日志 ID", e);
        }
    }

    /**
     * 获取最新的增量日志 ID，该 ID 与最新的快照 ID 相同。
     * @return 最新的增量日志 ID，如果不存在则返回 null
     */
    public @Nullable Long latestChangelogId() {
        return latestSnapshotId();
    }

    /**
     * 根据指定条件选择快照的 ID，如果符合条件的快照不存在则返回最新的快照 ID。
     * @param predicate 选择条件
     * @return 符合条件的快照 ID 或最新的快照 ID
     */
    public @Nullable Long pickOrLatest(Predicate<Snapshot> predicate) {
        Long latestId = latestSnapshotId();
        Long earliestId = earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return null;
        }

        for (long snapshotId = latestId; snapshotId >= earliestId; snapshotId--) {
            if (snapshotExists(snapshotId)) {
                Snapshot snapshot = snapshot(snapshotId);
                if (predicate.test(snapshot)) {
                    return snapshot.id();
                }
            }
        }

        return latestId;
    }

    /**
     * 根据时间戳，查找最早的不小于指定时间的快照 ID。
     * @param timestampMills 时间戳
     * @param startFromChangelog 是否从增量日志开始查找
     * @return 符合条件的快照 ID 或 null
     */
    private Snapshot changelogOrSnapshot(long snapshotId) {
        if (longLivedChangelogExists(snapshotId)) {
            return changelog(snapshotId);
        } else {
            return snapshot(snapshotId);
        }
    }

    /**
     * Returns the latest snapshot earlier than the timestamp mills. A non-existent snapshot may be
     * returned if all snapshots are equal to or later than the timestamp mills.
     */
    public @Nullable Long earlierThanTimeMills(long timestampMills, boolean startFromChangelog) {
        Long earliestSnapshot = earliestSnapshotId();
        Long earliest;
        if (startFromChangelog) {
            Long earliestChangelog = earliestLongLivedChangelogId();
            earliest = earliestChangelog == null ? earliestSnapshot : earliestChangelog;
        } else {
            earliest = earliestSnapshot;
        }
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        if (changelogOrSnapshot(earliest).timeMillis() >= timestampMills) {
            return earliest - 1;
        }

        while (earliest < latest) {
            long mid = (earliest + latest + 1) / 2;
            if (changelogOrSnapshot(mid).timeMillis() < timestampMills) {
                earliest = mid;
            } else {
                latest = mid - 1;
            }
        }
        return earliest;
    }


    /**
     * 返回一个快照，该快照的提交时间早于或等于给定的时间毫秒。
     * 如果没有这样的快照，则返回 null。
     * @param timestampMills 时间戳（毫秒）
     * @return 符合条件的快照
     */
    public @Nullable Snapshot earlierOrEqualTimeMills(long timestampMills) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        Snapshot earliestSnapShot = snapshot(earliest);
        if (earliestSnapShot.timeMillis() > timestampMills) {
            return earliestSnapShot;
        }
        Snapshot finalSnapshot = null;
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // 避免溢出
            Snapshot snapshot = snapshot(mid);
            long commitTime = snapshot.timeMillis();
            if (commitTime > timestampMills) {
                latest = mid - 1; // 搜索左半部分
            } else if (commitTime < timestampMills) {
                earliest = mid + 1; // 搜索右半部分
                finalSnapshot = snapshot;
            } else {
                finalSnapshot = snapshot; // 找到完全匹配
                break;
            }
        }
        return finalSnapshot;
    }

    /**
     * 返回一个快照，该快照的提交时间不早于给定的时间毫秒。
     * 如果没有这样的快照，则返回 null。
     * @param timestampMills 时间戳（毫秒）
     * @return 符合条件的快照
     */
    public @Nullable Snapshot laterOrEqualTimeMills(long timestampMills) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        Snapshot latestSnapShot = snapshot(latest);
        if (latestSnapShot.timeMillis() < timestampMills) {
            return null;
        }
        Snapshot finalSnapshot = null;
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // 避免溢出
            Snapshot snapshot = snapshot(mid);
            long commitTime = snapshot.timeMillis();
            if (commitTime > timestampMills) {
                latest = mid - 1; // 搜索左半部分
                finalSnapshot = snapshot;
            } else if (commitTime < timestampMills) {
                earliest = mid + 1; // 搜索右半部分
            } else {
                finalSnapshot = snapshot; // 找到完全匹配
                break;
            }
        }
        return finalSnapshot;
    }

    /**
     * 根据水印查找快照。
     * 如果找不到符合条件的快照，则返回 null。
     * @param watermark 水印
     * @return 符合条件的快照
     */
    public @Nullable Snapshot laterOrEqualWatermark(long watermark) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null || snapshot(latest).watermark() == Long.MIN_VALUE) {
            return null;
        }
        Long earliestWatermark = null;
        if ((earliestWatermark = snapshot(earliest).watermark()) == null) {
            while (earliest < latest) {
                earliest++;
                earliestWatermark = snapshot(earliest).watermark();
                if (earliestWatermark != null) {
                    break;
                }
            }
        }
        if (earliestWatermark == null) {
            return null;
        }

        if (earliestWatermark >= watermark) {
            return snapshot(earliest);
        }
        Snapshot finalSnapshot = null;

        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // 避免溢出
            Snapshot snapshot = snapshot(mid);
            Long commitWatermark = snapshot.watermark();
            if (commitWatermark == null) {
                // 查找第一个带水印的快照
                while (mid >= earliest) {
                    mid--;
                    commitWatermark = snapshot(mid).watermark();
                    if (commitWatermark != null) {
                        break;
                    }
                }
            }
            if (commitWatermark == null) {
                earliest = mid + 1;
            } else {
                if (commitWatermark > watermark) {
                    latest = mid - 1; // 搜索左半部分
                    finalSnapshot = snapshot;
                } else if (commitWatermark < watermark) {
                    earliest = mid + 1; // 搜索右半部分
                } else {
                    finalSnapshot = snapshot; // 找到完全匹配
                    break;
                }
            }
        }
        return finalSnapshot;
    }

    public long snapshotCount() throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX).count();
    }

    public Iterator<Snapshot> snapshots() throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                .map(id -> snapshot(id))
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public Iterator<Snapshot> snapshotsWithinRange(
            Optional<Long> optionalMaxSnapshotId, Optional<Long> optionalMinSnapshotId)
            throws IOException {
        Long lowerBoundSnapshotId = earliestSnapshotId();
        Long upperBoundSnapshotId = latestSnapshotId();

        if (lowerBoundSnapshotId == null || upperBoundSnapshotId == null) {
            return Collections.emptyIterator();
        }

        if (optionalMaxSnapshotId.isPresent()) {
            upperBoundSnapshotId = optionalMaxSnapshotId.get();
        }

        if (optionalMinSnapshotId.isPresent()) {
            lowerBoundSnapshotId = optionalMinSnapshotId.get();
        }

        return LongStream.range(lowerBoundSnapshotId, upperBoundSnapshotId + 1)
                .mapToObj(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public Iterator<Changelog> changelogs() throws IOException {
        return listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                .map(snapshotId -> changelog(snapshotId))
                .sorted(Comparator.comparingLong(Changelog::id))
                .iterator();
    }

    /**
     * 如果在读取快照文件时发生文件未找到的异常，则跳过此快照。
     * @return 所有可用的快照
     * @throws IOException 如果发生其他 I/O 错误
     */
    public List<Snapshot> safelyGetAllSnapshots() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX)
                        .map(id -> snapshotPath(id))
                        .collect(Collectors.toList());

        List<Snapshot> snapshots = new ArrayList<>();
        for (Path path : paths) {
            try {
                snapshots.add(Snapshot.fromJson(fileIO.readFileUtf8(path)));
            } catch (FileNotFoundException ignored) {
            }
        }

        return snapshots;
    }

    public List<Changelog> safelyGetAllChangelogs() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                        .map(id -> longLivedChangelogPath(id))
                        .collect(Collectors.toList());

        List<Changelog> changelogs = new ArrayList<>();
        for (Path path : paths) {
            try {
                String json = fileIO.readFileUtf8(path);
                changelogs.add(Changelog.fromJson(json));
            } catch (FileNotFoundException ignored) {
            }
        }

        return changelogs;
    }

    /**
     * 尝试获取非快照文件，如果出现错误则返回一个空列表。
     * @param fileStatusFilter 文件状态过滤器
     * @return 非快照文件路径列表
     */
    public List<Path> tryGetNonSnapshotFiles(Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(snapshotDirectory(), fileStatusFilter, nonSnapshotFileFilter());
    }

    public List<Path> tryGetNonChangelogFiles(Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(changelogDirectory(), fileStatusFilter, nonChangelogFileFilter());
    }

    private List<Path> listPathWithFilter(
            Path directory, Predicate<FileStatus> fileStatusFilter, Predicate<Path> fileFilter) {
        try {
            FileStatus[] statuses = fileIO.listStatus(directory);
            if (statuses == null) {
                return Collections.emptyList();
            }

            return Arrays.stream(statuses)
                    .filter(fileStatusFilter)
                    .map(FileStatus::getPath)
                    .filter(fileFilter)
                    .collect(Collectors.toList());
        } catch (IOException ignored) {
            return Collections.emptyList();
        }
    }

    private Predicate<Path> nonSnapshotFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(SNAPSHOT_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    private Predicate<Path> nonChangelogFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(CHANGELOG_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    public Optional<Snapshot> latestSnapshotOfUser(String user) {
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return Optional.empty();
        }

        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot;
            try {
                snapshot = snapshot(id);
            } catch (Exception e) {
                long newEarliestId =
                        Preconditions.checkNotNull(
                                earliestSnapshotId(),
                                "Latest snapshot id is not null, but earliest snapshot id is null. "
                                        + "This is unexpected.");

                if (id >= newEarliestId) {
                    throw e;
                }

                LOG.warn(
                        "Snapshot #" + id + " is expired. The latest snapshot of user(" + user + ") is not found.");
                break;
            }

            if (user.equals(snapshot.commitUser())) {
                return Optional.of(snapshot);
            }
        }
        return Optional.empty();
    }

    /**
     * 根据用户和标识符查找快照列表。
     * @param user 用户
     * @param identifiers 标识符列表
     * @return 符合条件的快照列表
     */
    public List<Snapshot> findSnapshotsForIdentifiers(
            @Nonnull String user, List<Long> identifiers) {
        if (identifiers.isEmpty()) {
            return Collections.emptyList();
        }
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return Collections.emptyList();
        }
        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(),
                        "Latest snapshot id is not null, but earliest snapshot id is null. This is unexpected.");

        long minSearchedIdentifier = identifiers.stream().min(Long::compareTo).get();
        List<Snapshot> matchedSnapshots = new ArrayList<>();
        Set<Long> remainingIdentifiers = new HashSet<>(identifiers);
        for (long id = latestId; id >= earliestId && !remainingIdentifiers.isEmpty(); id--) {
            Snapshot snapshot = snapshot(id);
            if (user.equals(snapshot.commitUser())) {
                if (remainingIdentifiers.remove(snapshot.commitIdentifier())) {
                    matchedSnapshots.add(snapshot);
                }
                if (snapshot.commitIdentifier() <= minSearchedIdentifier) {
                    break;
                }
            }
        }
        return matchedSnapshots;
    }

    public void commitChangelog(Changelog changelog, long id) throws IOException {
        fileIO.writeFile(longLivedChangelogPath(id), changelog.toJson(), false);
    }

    /**
     * 安全地从最新快照开始遍历，适用于快照可能被删除的场景。
     * @param checker 快照检查器
     * @return 符合条件的快照
     */
    @Nullable
    public Snapshot traversalSnapshotsFromLatestSafely(Filter<Snapshot> checker) {
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return null;
        }
        Long earliestId = earliestSnapshotId();
        if (earliestId == null) {
            return null;
        }

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot;
            try {
                snapshot = snapshot(id);
            } catch (Exception e) {
                Long newEarliestId = earliestSnapshotId();
                if (newEarliestId == null) {
                    return null;
                }

                // this is a valid snapshot, should throw exception
                if (id >= newEarliestId) {
                    throw e;
                }

                // ok, this is an expired snapshot
                return null;
            }

            if (checker.test(snapshot)) {
                return snapshot;
            }
        }
        return null;
    }

    private @Nullable Long findLatest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        Long snapshotId = readHint(LATEST, dir);
        if (snapshotId != null && snapshotId > 0) {
            long nextSnapshot = snapshotId + 1;
            // it is the latest only there is no next one
            if (!fileIO.exists(file.apply(nextSnapshot))) {
                return snapshotId;
            }
        }
        return findByListFiles(Math::max, dir, prefix);
    }

    private @Nullable Long findEarliest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        Long snapshotId = readHint(EARLIEST, dir);
        // null and it is the earliest only it exists
        if (snapshotId != null && fileIO.exists(file.apply(snapshotId))) {
            return snapshotId;
        }

        return findByListFiles(Math::min, dir, prefix);
    }

    public Long readHint(String fileName) {
        return readHint(fileName, snapshotDirectory());
    }

    public Long readHint(String fileName, Path dir) {
        Path path = new Path(dir, fileName);
        int retryNumber = 0;
        while (retryNumber++ < READ_HINT_RETRY_NUM) {
            try {
                return fileIO.readOverwrittenFileUtf8(path).map(Long::parseLong).orElse(null);
            } catch (Exception ignored) {
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_HINT_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private Long findByListFiles(BinaryOperator<Long> reducer, Path dir, String prefix)
            throws IOException {
        return listVersionedFiles(fileIO, dir, prefix).reduce(reducer).orElse(null);
    }

    /**
     * 根据开始和结束 ID 查找重叠的快照。
     * @param sortedSnapshots 排序后的快照列表
     * @param beginInclusive 开始 ID（包含）
     * @param endExclusive 结束 ID（不包含）
     * @return 重叠的快照列表
     */
    public static List<Snapshot> findOverlappedSnapshots(
            List<Snapshot> sortedSnapshots, long beginInclusive, long endExclusive) {
        List<Snapshot> overlappedSnapshots = new ArrayList<>();
        int right = findPreviousSnapshot(sortedSnapshots, endExclusive);
        if (right >= 0) {
            int left = Math.max(findPreviousOrEqualSnapshot(sortedSnapshots, beginInclusive), 0);
            for (int i = left; i <= right; i++) {
                overlappedSnapshots.add(sortedSnapshots.get(i));
            }
        }
        return overlappedSnapshots;
    }

    public static int findPreviousSnapshot(List<Snapshot> sortedSnapshots, long targetSnapshotId) {
        for (int i = sortedSnapshots.size() - 1; i >= 0; i--) {
            if (sortedSnapshots.get(i).id() < targetSnapshotId) {
                return i;
            }
        }
        return -1;
    }

    private static int findPreviousOrEqualSnapshot(
            List<Snapshot> sortedSnapshots, long targetSnapshotId) {
        for (int i = sortedSnapshots.size() - 1; i >= 0; i--) {
            if (sortedSnapshots.get(i).id() <= targetSnapshotId) {
                return i;
            }
        }
        return -1;
    }

    public void deleteLatestHint() throws IOException {
        Path snapshotDir = snapshotDirectory();
        Path hintFile = new Path(snapshotDir, LATEST);
        fileIO.delete(hintFile, false);
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, LATEST, snapshotDirectory());
    }

    public void commitLongLivedChangelogLatestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, LATEST, changelogDirectory());
    }

    public void commitLongLivedChangelogEarliestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, EARLIEST, changelogDirectory());
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, EARLIEST, snapshotDirectory());
    }

    private void commitHint(long snapshotId, String fileName, Path dir) throws IOException {
        Path hintFile = new Path(dir, fileName);
        fileIO.overwriteFileUtf8(hintFile, String.valueOf(snapshotId));
    }
}
