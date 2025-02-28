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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.schema.SchemaChange.AddColumn;
import org.apache.paimon.schema.SchemaChange.DropColumn;
import org.apache.paimon.schema.SchemaChange.RemoveOption;
import org.apache.paimon.schema.SchemaChange.RenameColumn;
import org.apache.paimon.schema.SchemaChange.SetOption;
import org.apache.paimon.schema.SchemaChange.UpdateColumnComment;
import org.apache.paimon.schema.SchemaChange.UpdateColumnNullability;
import org.apache.paimon.schema.SchemaChange.UpdateColumnPosition;
import org.apache.paimon.schema.SchemaChange.UpdateColumnType;
import org.apache.paimon.schema.SchemaChange.UpdateComment;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Catalog.DB_SUFFIX;
import static org.apache.paimon.catalog.Identifier.UNKNOWN_DATABASE;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * SchemaManager 用于管理 Schema 的版本。
 * 这个类支持 Schema 的版本管理，包括读取、写入、更新和清理等操作。
 * 它通过文件系统存储 Schema，支持多分支管理，并提供了并发控制机制。
 */
@ThreadSafe
public class SchemaManager implements Serializable {

    // 前缀用于标识 schema 文件
    private static final String SCHEMA_PREFIX = "schema-";

    // 文件 IO 操作工具
    private final FileIO fileIO;

    // 表的根目录路径
    private final Path tableRoot;

    // 可选的锁，用于并发控制
    @Nullable private transient Lock lock;

    // 当前分支名称
    private final String branch;

    // 构造函数，使用默认分支
    public SchemaManager(FileIO fileIO, Path tableRoot) {
        this(fileIO, tableRoot, DEFAULT_MAIN_BRANCH);
    }

    /**
     * 构造函数，指定分支名称。
     * 如果传入的分支名称为空，则使用默认分支。
     */
    public SchemaManager(FileIO fileIO, Path tableRoot, String branch) {
        this.fileIO = fileIO;
        this.tableRoot = tableRoot;
        this.branch = StringUtils.isBlank(branch) ? DEFAULT_MAIN_BRANCH : branch;
    }

    // 创建一个新的 SchemaManager 实例，指定分支名称
    public SchemaManager copyWithBranch(String branchName) {
        return new SchemaManager(fileIO, tableRoot, branchName);
    }

    // 设置锁，用于并发控制
    public SchemaManager withLock(@Nullable Lock lock) {
        this.lock = lock;
        return this;
    }

    /**
     * 获取最新的 schema。
     * 如果不存在任何 schema，则返回空。
     */
    public Optional<TableSchema> latest() {
        try {
            // 列出所有版本化的文件，取最大版本号对应的 schema
            return listVersionedFiles(fileIO, schemaDirectory(), SCHEMA_PREFIX)
                    .reduce(Math::max)
                    .map(this::schema);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // 获取所有 schema 的列表
    public List<TableSchema> listAll() {
        return listAllIds().stream().map(this::schema).collect(Collectors.toList());
    }

    /**
     * 列出所有 schema 的 ID。
     * 如果不存在 schema，则返回空列表。
     */
    public List<Long> listAllIds() {
        try {
            // 列出所有版本化的文件，返回版本号列表
            return listVersionedFiles(fileIO, schemaDirectory(), SCHEMA_PREFIX)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 创建表的 schema。
     * 如果表已存在且 `ignoreIfExistsSame` 为 true，则返回已存在的 schema。
     * 如果表已存在且 schema 不同，则抛出异常。
     */
    public TableSchema createTable(Schema schema) throws Exception {
        return createTable(schema, false);
    }

    public TableSchema createTable(Schema schema, boolean ignoreIfExistsSame) throws Exception {
        while (true) {
            // 获取最新的 schema
            Optional<TableSchema> latest = latest();
            if (latest.isPresent()) {
                TableSchema oldSchema = latest.get();
                // 比较新旧 schema 是否相同
                boolean isSame =
                        Objects.equals(oldSchema.fields(), schema.fields())
                                && Objects.equals(oldSchema.partitionKeys(), schema.partitionKeys())
                                && Objects.equals(oldSchema.primaryKeys(), schema.primaryKeys())
                                && Objects.equals(oldSchema.options(), schema.options());
                if (ignoreIfExistsSame && isSame) {
                    return oldSchema;
                }

                throw new IllegalStateException(
                        "Schema in filesystem exists, please use updating,"
                                + " latest schema is: "
                                + oldSchema);
            }

            // 如果不存在 schema，则创建新的 schema
            List<DataField> fields = schema.fields();
            List<String> partitionKeys = schema.partitionKeys();
            List<String> primaryKeys = schema.primaryKeys();
            Map<String, String> options = schema.options();
            int highestFieldId = RowType.currentHighestFieldId(fields);

            TableSchema newSchema =
                    new TableSchema(
                            0,
                            fields,
                            highestFieldId,
                            partitionKeys,
                            primaryKeys,
                            options,
                            schema.comment());

            boolean success = commit(newSchema);
            if (success) {
                return newSchema;
            }
        }
    }

    /**
     * 提交 schema 的变更。
     * 支持多种类型的变更，例如添加列、删除列、修改列类型等。
     */
    public TableSchema commitChanges(SchemaChange... changes) throws Exception {
        return commitChanges(Arrays.asList(changes));
    }

    public TableSchema commitChanges(List<SchemaChange> changes)
            throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException,
            Catalog.ColumnNotExistException {
        // 获取快照管理器
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tableRoot, branch);
        boolean hasSnapshots = (snapshotManager.latestSnapshotId() != null);

        while (true) {
            // 获取最新的 schema
            TableSchema oldTableSchema =
                    latest().orElseThrow(
                            () ->
                                    new Catalog.TableNotExistException(
                                            identifierFromPath(
                                                    tableRoot.toString(), true, branch)));
            // 复制旧的 schema 的选项和字段
            Map<String, String> oldOptions = new HashMap<>(oldTableSchema.options());
            Map<String, String> newOptions = new HashMap<>(oldTableSchema.options());
            List<DataField> newFields = new ArrayList<>(oldTableSchema.fields());
            AtomicInteger highestFieldId = new AtomicInteger(oldTableSchema.highestFieldId());
            String newComment = oldTableSchema.comment();

            // 遍历所有变更，更新 schema
            for (SchemaChange change : changes) {
                if (change instanceof SetOption) {
                    // 设置选项
                    SetOption setOption = (SetOption) change;
                    if (hasSnapshots) {
                        checkAlterTableOption(
                                setOption.key(),
                                oldOptions.get(setOption.key()),
                                setOption.value(),
                                false);
                    }
                    newOptions.put(setOption.key(), setOption.value());
                } else if (change instanceof RemoveOption) {
                    // 删除选项
                    RemoveOption removeOption = (RemoveOption) change;
                    if (hasSnapshots) {
                        checkResetTableOption(removeOption.key());
                    }
                    newOptions.remove(removeOption.key());
                } else if (change instanceof UpdateComment) {
                    // 更新注释
                    UpdateComment updateComment = (UpdateComment) change;
                    newComment = updateComment.comment();
                } else if (change instanceof AddColumn) {
                    // 添加列
                    AddColumn addColumn = (AddColumn) change;
                    SchemaChange.Move move = addColumn.move();
                    if (newFields.stream().anyMatch(f -> f.name().equals(addColumn.fieldName()))) {
                        throw new Catalog.ColumnAlreadyExistException(
                                identifierFromPath(tableRoot.toString(), true, branch),
                                addColumn.fieldName());
                    }
                    Preconditions.checkArgument(
                            addColumn.dataType().isNullable(),
                            "Column %s cannot specify NOT NULL in the %s table.",
                            addColumn.fieldName(),
                            identifierFromPath(tableRoot.toString(), true, branch).getFullName());
                    int id = highestFieldId.incrementAndGet();
                    DataType dataType =
                            ReassignFieldId.reassign(addColumn.dataType(), highestFieldId);

                    DataField dataField =
                            new DataField(
                                    id, addColumn.fieldName(), dataType, addColumn.description());

                    // 根据指定位置插入列
                    Map<String, Integer> map = new HashMap<>();
                    for (int i = 0; i < newFields.size(); i++) {
                        map.put(newFields.get(i).name(), i);
                    }

                    if (null != move) {
                        if (move.type().equals(SchemaChange.Move.MoveType.FIRST)) {
                            newFields.add(0, dataField);
                        } else if (move.type().equals(SchemaChange.Move.MoveType.AFTER)) {
                            int fieldIndex = map.get(move.referenceFieldName());
                            newFields.add(fieldIndex + 1, dataField);
                        }
                    } else {
                        newFields.add(dataField);
                    }
                } else if (change instanceof RenameColumn) {
                    // 重命名列
                    RenameColumn rename = (RenameColumn) change;
                    validateNotPrimaryAndPartitionKey(oldTableSchema, rename.fieldName());
                    if (newFields.stream().anyMatch(f -> f.name().equals(rename.newName()))) {
                        throw new Catalog.ColumnAlreadyExistException(
                                identifierFromPath(tableRoot.toString(), true, branch),
                                rename.fieldName());
                    }

                    updateNestedColumn(
                            newFields,
                            new String[] {rename.fieldName()},
                            0,
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            rename.newName(),
                                            field.type(),
                                            field.description()));
                } else if (change instanceof DropColumn) {
                    // 删除列
                    DropColumn drop = (DropColumn) change;
                    validateNotPrimaryAndPartitionKey(oldTableSchema, drop.fieldName());
                    if (!newFields.removeIf(
                            f -> f.name().equals(((DropColumn) change).fieldName()))) {
                        throw new Catalog.ColumnNotExistException(
                                identifierFromPath(tableRoot.toString(), true, branch),
                                drop.fieldName());
                    }
                    if (newFields.isEmpty()) {
                        throw new IllegalArgumentException("Cannot drop all fields in table");
                    }
                } else if (change instanceof UpdateColumnType) {
                    // 更新列类型
                    UpdateColumnType update = (UpdateColumnType) change;
                    if (oldTableSchema.partitionKeys().contains(update.fieldName())) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Cannot update partition column [%s] type in the table[%s].",
                                        update.fieldName(), tableRoot.getName()));
                    }
                    updateColumn(
                            newFields,
                            update.fieldName(),
                            (field) -> {
                                DataType targetType = update.newDataType();
                                if (update.keepNullability()) {
                                    targetType = targetType.copy(field.type().isNullable());
                                }
                                checkState(
                                        DataTypeCasts.supportsExplicitCast(field.type(), targetType)
                                                && CastExecutors.resolve(field.type(), targetType)
                                                != null,
                                        String.format(
                                                "Column type %s[%s] cannot be converted to %s without loosing information.",
                                                field.name(), field.type(), targetType));
                                AtomicInteger dummyId = new AtomicInteger(0);
                                if (dummyId.get() != 0) {
                                    throw new RuntimeException(
                                            String.format(
                                                    "Update column to nested row type '%s' is not supported.",
                                                    targetType));
                                }
                                return new DataField(
                                        field.id(), field.name(), targetType, field.description());
                            });
                } else if (change instanceof UpdateColumnNullability) {
                    // 更新列的可空性
                    UpdateColumnNullability update = (UpdateColumnNullability) change;
                    if (update.fieldNames().length == 1
                            && update.newNullability()
                            && oldTableSchema.primaryKeys().contains(update.fieldNames()[0])) {
                        throw new UnsupportedOperationException(
                                "Cannot change nullability of primary key");
                    }
                    updateNestedColumn(
                            newFields,
                            update.fieldNames(),
                            0,
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            field.name(),
                                            field.type().copy(update.newNullability()),
                                            field.description()));
                } else if (change instanceof UpdateColumnComment) {
                    // 更新列的注释
                    UpdateColumnComment update = (UpdateColumnComment) change;
                    updateNestedColumn(
                            newFields,
                            update.fieldNames(),
                            0,
                            (field) ->
                                    new DataField(
                                            field.id(),
                                            field.name(),
                                            field.type(),
                                            update.newDescription()));
                } else if (change instanceof UpdateColumnPosition) {
                    // 更新列的位置
                    UpdateColumnPosition update = (UpdateColumnPosition) change;
                    SchemaChange.Move move = update.move();
                    applyMove(newFields, move);
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported change: " + change.getClass());
                }
            }

            // 创建新的 schema
            Schema newSchema =
                    new Schema(
                            newFields,
                            oldTableSchema.partitionKeys(),
                            oldTableSchema.primaryKeys(),
                            newOptions,
                            newComment);
            TableSchema newTableSchema =
                    new TableSchema(
                            oldTableSchema.id() + 1,
                            newSchema.fields(),
                            highestFieldId.get(),
                            newSchema.partitionKeys(),
                            newSchema.primaryKeys(),
                            newSchema.options(),
                            newSchema.comment());

            // 提交新的 schema
            try {
                boolean success = commit(newTableSchema);
                if (success) {
                    return newTableSchema;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 移动列的位置
    public void applyMove(List<DataField> newFields, SchemaChange.Move move) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < newFields.size(); i++) {
            map.put(newFields.get(i).name(), i);
        }

        int fieldIndex = map.getOrDefault(move.fieldName(), -1);
        if (fieldIndex == -1) {
            throw new IllegalArgumentException("Field name not found: " + move.fieldName());
        }

        // 处理 FIRST 和 LAST 的情况
        switch (move.type()) {
            case FIRST:
                checkMoveIndexEqual(move, fieldIndex, 0);
                moveField(newFields, fieldIndex, 0);
                return;
            case LAST:
                checkMoveIndexEqual(move, fieldIndex, newFields.size() - 1);
                moveField(newFields, fieldIndex, newFields.size() - 1);
                return;
        }

        Integer refIndex = map.getOrDefault(move.referenceFieldName(), -1);
        if (refIndex == -1) {
            throw new IllegalArgumentException(
                    "Reference field name not found: " + move.referenceFieldName());
        }

        checkMoveIndexEqual(move, fieldIndex, refIndex);

        // 根据移动类型调整目标索引
        int targetIndex = refIndex;
        if (move.type() == SchemaChange.Move.MoveType.AFTER && fieldIndex > refIndex) {
            targetIndex++;
        }
        if (move.type() == SchemaChange.Move.MoveType.BEFORE && fieldIndex < refIndex) {
            targetIndex--;
        }

        if (targetIndex > (newFields.size() - 1)) {
            targetIndex = newFields.size() - 1;
        }

        moveField(newFields, fieldIndex, targetIndex);
    }

    // 移动字段的工具方法
    private void moveField(List<DataField> newFields, int fromIndex, int toIndex) {
        if (fromIndex < 0 || fromIndex >= newFields.size() || toIndex < 0) {
            return;
        }
        DataField fieldToMove = newFields.remove(fromIndex);
        newFields.add(toIndex, fieldToMove);
    }

    // 检查移动索引是否合法
    private static void checkMoveIndexEqual(SchemaChange.Move move, int fieldIndex, int refIndex) {
        if (refIndex == fieldIndex) {
            throw new UnsupportedOperationException(
                    String.format("Cannot move itself for column %s", move.fieldName()));
        }
    }

    // 合并 schema
    public boolean mergeSchema(RowType rowType, boolean allowExplicitCast) {
        TableSchema current =
                latest().orElseThrow(
                        () ->
                                new RuntimeException(
                                        "It requires that the current schema to exist when calling 'mergeSchema'"));
        TableSchema update = SchemaMergingUtils.mergeSchemas(current, rowType, allowExplicitCast);
        if (current.equals(update)) {
            return false;
        } else {
            try {
                return commit(update);
            } catch (Exception e) {
                throw new RuntimeException("Failed to commit the schema.", e);
            }
        }
    }

    // 验证字段是否为主键或分区键
    private void validateNotPrimaryAndPartitionKey(TableSchema schema, String fieldName) {
        if (schema.partitionKeys().contains(fieldName)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot drop/rename partition key[%s]", fieldName));
        }
        if (schema.primaryKeys().contains(fieldName)) {
            throw new UnsupportedOperationException(
                    String.format("Cannot drop/rename primary key[%s]", fieldName));
        }
    }

    // 更新嵌套列
    private void updateNestedColumn(
            List<DataField> newFields,
            String[] updateFieldNames,
            int index,
            Function<DataField, DataField> updateFunc)
            throws Catalog.ColumnNotExistException {
        boolean found = false;
        for (int i = 0; i < newFields.size(); i++) {
            DataField field = newFields.get(i);
            if (field.name().equals(updateFieldNames[index])) {
                found = true;
                if (index == updateFieldNames.length - 1) {
                    newFields.set(i, updateFunc.apply(field));
                    break;
                } else {
                    List<DataField> nestedFields =
                            new ArrayList<>(
                                    ((org.apache.paimon.types.RowType) field.type()).getFields());
                    updateNestedColumn(nestedFields, updateFieldNames, index + 1, updateFunc);
                    newFields.set(
                            i,
                            new DataField(
                                    field.id(),
                                    field.name(),
                                    new org.apache.paimon.types.RowType(
                                            field.type().isNullable(), nestedFields),
                                    field.description()));
                }
            }
        }
        if (!found) {
            throw new Catalog.ColumnNotExistException(
                    identifierFromPath(tableRoot.toString(), true, branch),
                    Arrays.toString(updateFieldNames));
        }
    }

    // 更新列
    private void updateColumn(
            List<DataField> newFields,
            String updateFieldName,
            Function<DataField, DataField> updateFunc)
            throws Catalog.ColumnNotExistException {
        updateNestedColumn(newFields, new String[] {updateFieldName}, 0, updateFunc);
    }

    // 提交新的 schema
    @VisibleForTesting
    boolean commit(TableSchema newSchema) throws Exception {
        // 验证 schema
        SchemaValidation.validateTableSchema(newSchema);
        SchemaValidation.validateFallbackBranch(this, newSchema);
        Path schemaPath = toSchemaPath(newSchema.id());
        Callable<Boolean> callable =
                () -> fileIO.tryToWriteAtomic(schemaPath, newSchema.toString());
        if (lock == null) {
            return callable.call();
        }
        return lock.runWithLock(callable);
    }

    // 根据 schema ID 获取 schema
    public TableSchema schema(long id) {
        try {
            return JsonSerdeUtil.fromJson(fileIO.readFileUtf8(toSchemaPath(id)), TableSchema.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // 检查 schema 是否存在
    public boolean schemaExists(long id) {
        Path path = toSchemaPath(id);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to determine if schema '%s' exists in path %s.", id, path),
                    e);
        }
    }

    // 从路径加载 schema
    public static TableSchema fromPath(FileIO fileIO, Path path) {
        try {
            return JsonSerdeUtil.fromJson(fileIO.readFileUtf8(path), TableSchema.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // 获取分支路径
    private String branchPath() {
        return BranchManager.branchPath(tableRoot, branch);
    }

    // 获取 schema 目录
    public Path schemaDirectory() {
        return new Path(branchPath() + "/schema");
    }

    // 获取 schema 文件路径
    @VisibleForTesting
    public Path toSchemaPath(long schemaId) {
        return new Path(branchPath() + "/schema/" + SCHEMA_PREFIX + schemaId);
    }

    // 删除指定 ID 的 schema
    public void deleteSchema(long schemaId) {
        fileIO.deleteQuietly(toSchemaPath(schemaId));
    }

    // 检查是否可以修改表的选项
    public static void checkAlterTableOption(
            String key, @Nullable String oldValue, String newValue, boolean fromDynamicOptions) {
        if (CoreOptions.IMMUTABLE_OPTIONS.contains(key)) {
            throw new UnsupportedOperationException(
                    String.format("Change '%s' is not supported yet.", key));
        }

        if (CoreOptions.BUCKET.key().equals(key)) {
            int oldBucket =
                    oldValue == null
                            ? CoreOptions.BUCKET.defaultValue()
                            : Integer.parseInt(oldValue);
            int newBucket = Integer.parseInt(newValue);

            if (fromDynamicOptions) {
                throw new UnsupportedOperationException(
                        "Cannot change bucket number through dynamic options. You might need to rescale bucket.");
            }
            if (oldBucket == -1) {
                throw new UnsupportedOperationException("Cannot change bucket when it is -1.");
            }
            if (newBucket == -1) {
                throw new UnsupportedOperationException("Cannot change bucket to -1.");
            }
        }
    }

    // 检查是否可以重置表的选项
    public static void checkResetTableOption(String key) {
        if (CoreOptions.IMMUTABLE_OPTIONS.contains(key)) {
            throw new UnsupportedOperationException(
                    String.format("Change '%s' is not supported yet.", key));
        }

        if (CoreOptions.BUCKET.key().equals(key)) {
            throw new UnsupportedOperationException(String.format("Cannot reset %s.", key));
        }
    }

    // 检查是否可以修改表的路径
    public static void checkAlterTablePath(String key) {
        if (CoreOptions.PATH.key().equalsIgnoreCase(key)) {
            throw new UnsupportedOperationException("Change path is not supported yet.");
        }
    }

    // 从路径生成表的标识符
    public static Identifier identifierFromPath(String tablePath, boolean ignoreIfUnknownDatabase) {
        return identifierFromPath(tablePath, ignoreIfUnknownDatabase, null);
    }

    public static Identifier identifierFromPath(
            String tablePath, boolean ignoreIfUnknownDatabase, @Nullable String branchName) {
        if (DEFAULT_MAIN_BRANCH.equals(branchName)) {
            branchName = null;
        }

        String[] paths = tablePath.split("/");
        if (paths.length < 2) {
            if (!ignoreIfUnknownDatabase) {
                throw new IllegalArgumentException(
                        String.format(
                                "Path '%s' is not a valid path, please use catalog table path instead: 'warehouse_path/your_database.db/your_table'.",
                                tablePath));
            }
            return new Identifier(UNKNOWN_DATABASE, paths[0]);
        }

        String database = paths[paths.length - 2];
        int index = database.lastIndexOf(DB_SUFFIX);
        if (index == -1) {
            if (!ignoreIfUnknownDatabase) {
                throw new IllegalArgumentException(
                        String.format(
                                "Path '%s' is not a valid path, please use catalog table path instead: 'warehouse_path/your_database.db/your_table'.",
                                tablePath));
            }
            return new Identifier(UNKNOWN_DATABASE, paths[paths.length - 1], branchName, null);
        }
        database = database.substring(0, index);

        return new Identifier(database, paths[paths.length - 1], branchName, null);
    }
}
