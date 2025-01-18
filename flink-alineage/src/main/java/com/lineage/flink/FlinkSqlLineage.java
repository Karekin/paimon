package com.lineage.flink;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import java.util.List;
import java.util.Set;

public class FlinkSqlLineage {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        /**
         * 创建source表
         * 创建catalog
         * 创建库
         * 创建paimon表
         */
        tableEnv.executeSql("CREATE TABLE datagen (\n    " +
                "f_sequence INT,\n    " +
                "f_random String,\n   " +
                "f_random_str STRING " +
                ") WITH (" +
                "'connector' = 'datagen')");

        String catalog = "CREATE CATALOG paimon " +
                "WITH (\n" +
                "'type' = 'paimon',\n"+
                "'warehouse' = 'file:/D:/lakehouse/')";
        tableEnv.executeSql(catalog);
        tableEnv.executeSql("create database if not exists paimon.test ");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.ods_flink (\n" +
                " `id` bigint,\n" +
                " `name` String,\n" +
                " `dt`  string,\n" +
                "  PRIMARY KEY (id,dt) NOT ENFORCED\n"
                + ") partitioned by (dt) with  (\n"
                + "   'changelog-producer' = 'input',\n"
                + "   'bucket' = '2' \n" + ") ");

        //流处理脚本
        String sql = "insert into paimon.test.ods_flink select f_sequence,f_random,f_random_str from default_catalog.default_database.datagen ";

        /**
         * 获取StreamTableEnvironment具体的子类实现
         */
        StreamTableEnvironmentImpl tableEnvmpl = null;
        if(tableEnv instanceof StreamTableEnvironmentImpl){
            tableEnvmpl = (StreamTableEnvironmentImpl) tableEnv;
        }
        //调用flink封装好的方法解析流处理脚本语句(insert into ……)
        List<Operation> operations = tableEnvmpl.getParser().parse(sql);

        /**
         * 获取第一个操作对象Operation
         * Operation就是对我们insert into语句进行词法、语法解析、校验后的，一般到这一步不报错就代表我们的语句是可以执行的
         * 那么我们从这一步就可以解析获取表之间的依赖关系、字段之间的依赖关系就可以了。
         */
        Operation operation = operations.get(0);
        SinkModifyOperation sinkModifyOperation = null;
        //判断操作对象是否是SinkModifyOperation，因为只有insert into语句才会是SinkModifyOperation
        if(operation instanceof SinkModifyOperation){
            sinkModifyOperation = (SinkModifyOperation) operation;
        }
        //获取SinkModifyOperation的子节点，子节点就是我们的查询语句
        QueryOperation queryOperation = sinkModifyOperation.getChild();
        PlannerQueryOperation plannerQueryOperation = null;
        //判断子节点是否是PlannerQueryOperation，因为只有PlannerQueryOperation才有我们需要的依赖关系
        if(queryOperation instanceof PlannerQueryOperation){
            plannerQueryOperation = (PlannerQueryOperation) queryOperation;
        }
        //获取PlannerQueryOperation的CalciteTree，CalciteTree就是我们的依赖关系树
        RelNode relNode = plannerQueryOperation.getCalciteTree();
        //获取目标表名
        String targetTable = sinkModifyOperation.getContextResolvedTable().getIdentifier().asSummaryString();
        System.out.println(targetTable);
        System.out.println(relNode);
        //获取目标表的所有字段
        List<Column> columns = tableEnv.from(targetTable).getResolvedSchema().getColumns();
        //获取元数据提供统一的访问接口
        RelMetadataQuery metadataQuery = relNode.getCluster().getMetadataQuery();
        //获取目标表字段和源表字段的依赖关系
        for(int index = 0 ;index < columns.size();index++){
            //获取目标表字段
            Column targetColumn =  columns.get(index);
            //获取目标表字段和源表字段的依赖关系
            Set<RelColumnOrigin>  relColumnOriginSet =  metadataQuery.getColumnOrigins(relNode,index);
            //判断目标表字段是否有依赖关系
            for(RelColumnOrigin columnOrigin : relColumnOriginSet){
                //获取底层Origintable
                RelOptTable relOptTable = columnOrigin.getOriginTable();
                //获取源表名
                String sourceTable = String.join(".",relOptTable.getQualifiedName());
                //获取源表字段在源表中的位置
                int ordinal = columnOrigin.getOriginColumnOrdinal();
                //获取源表的所有字段
                List<Column> sourceColumns  =  ((TableSourceTable)relOptTable).contextResolvedTable().getResolvedSchema().getColumns();
                //获取源表字段
                Column sourceColumn = sourceColumns.get(ordinal);
                System.out.println("sourceTable:" + sourceTable + "," + sourceColumn.getName() + ", sinkTable:" + targetTable + ", " + targetColumn.getName());
            }
        }
    }
}
