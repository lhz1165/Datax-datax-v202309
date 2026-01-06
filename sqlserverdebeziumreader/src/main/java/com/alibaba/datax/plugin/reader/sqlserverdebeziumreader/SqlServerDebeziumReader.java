package com.alibaba.datax.plugin.reader.sqlserverdebeziumreader;

import ch.qos.logback.classic.Level;
import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.HintUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class SqlServerDebeziumReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.SQLServer;

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.originalConfig.set(Constant.FETCH_SIZE, Integer.MIN_VALUE);


            originalConfig.set(Constant.IS_TABLE_MODE, true);


            //1.设置jdbcurl
            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);
            boolean isTableMode = originalConfig.getBool(Constant.IS_TABLE_MODE);


            List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
            Object conn = conns.get(0);
            Configuration connConf = Configuration.from(conn.toString());
            connConf.getNecessaryValue(Key.JDBC_URL, DBUtilErrorCode.REQUIRED_VALUE);

            List<String> jdbcUrls = connConf.getList(Key.JDBC_URL, String.class);
            String jdbcUrl = DBUtil.chooseJdbcUrl(DATABASE_TYPE, jdbcUrls,
                    username, password, null, false);

            jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForReader(jdbcUrl);

            //connection[0].jdbcUrl = jdbcUrl
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, 0, Key.JDBC_URL), jdbcUrl);
            LOG.info("Available jdbcUrl:{}.", jdbcUrl);


            //2. 设置table
            // 对每一个connection 上配置的table 项进行解析(已对表名称进行了 " 处理的)
            List<String> tables = connConf.getList(Key.TABLE, String.class);
            List<String> expandedTables = TableExpandUtil.expandTableConf(DATABASE_TYPE, tables);
            if (expandedTables.isEmpty()) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.ILLEGAL_VALUE, String.format("您所配置的读取数据库表:%s 不正确. 因为DataX根据您的配置找不到这张表. 请检查您的配置并作出修改." +
                                "请先了解 DataX 配置.", StringUtils.join(tables, ",")));
            }
            //connection[0].table = tables
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, 0, Key.TABLE), expandedTables);
            originalConfig.set(Constant.TABLE_NUMBER_MARK, 1);


            //3. 设置列
            List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
            if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置读取数据库表的列信息. " +
                        "正确的配置方式是给 column 配置上您需要读取的列名称,用英文逗号分隔. 例如: \"column\": [\"id\", \"name\"],请参考上述配置并作出修改.");
            } else {
                String splitPk = originalConfig.getString(Key.SPLIT_PK, null);

                if (1 == userConfiguredColumns.size()
                        && "*".equals(userConfiguredColumns.get(0))) {
                    LOG.warn("您的配置文件中的列配置存在一定的风险. 因为您未配置读取数据库表的列，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");
                    // 回填其值，需要以 String 的方式转交后续处理
                    originalConfig.set(Key.COLUMN, "*");
                } else {
                    String tableName = originalConfig.getString(String.format("%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

                    List<String> allColumns = DBUtil.getTableColumns(
                            DATABASE_TYPE, jdbcUrl, username, password,
                            tableName);
                    LOG.info("table:[{}] has columns:[{}].",
                            tableName, StringUtils.join(allColumns, ","));
                    // warn:注意PostgreSQL表名区分大小写，但通常使用小写
                    allColumns = ListUtil.valueToLowerCase(allColumns);
                    List<String> quotedColumns = new ArrayList<String>();

                    for (String column : userConfiguredColumns) {
                        if ("*".equals(column)) {
                            throw DataXException.asDataXException(
                                    DBUtilErrorCode.ILLEGAL_VALUE,
                                    "您的配置文件中的列配置信息有误. 因为根据您的配置，数据库表的列中存在多个*. 请检查您的配置并作出修改. ");
                        }

                        quotedColumns.add(column);
                    }
                    originalConfig.set(Key.COLUMN_LIST, quotedColumns);
                    originalConfig.set(Key.COLUMN,
                            StringUtils.join(quotedColumns, ","));
                    if (StringUtils.isNotBlank(splitPk)) {
                        if (!allColumns.contains(splitPk.toLowerCase())) {
                            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                                    String.format("您的配置文件中的列配置信息有误. 因为根据您的配置，您读取的数据库表:%s 中没有主键名为:%s. 请检查您的配置并作出修改.", tableName, splitPk));
                        }
                    }
                }
            }

            LOG.debug("After job init(), job config now is:[\n{}\n]", originalConfig.toJSON());

        }

        @Override
        public void preCheck() {
            init();

        }


        @Override
        public void destroy() {

        }

        //把表名和列配置转化成查询 sql，消费者需要根据这个sql的列名来获取列
        @Override
        public List<Configuration> split(int adviceNumber) {
            int eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
                    adviceNumber, this.originalConfig.getInt(Constant.TABLE_NUMBER_MARK));

            String column = originalConfig.getString(Key.COLUMN);
            String where = originalConfig.getString(Key.WHERE, null);
            List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);

            List<Configuration> splittedConfigs = new ArrayList<Configuration>();
            for (int i = 0, len = conns.size(); i < len; i++) {
                Configuration sliceConfig = originalConfig.clone();
                Configuration connConf = Configuration.from(conns.get(i).toString());
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                sliceConfig.set(Key.JDBC_URL, jdbcUrl);
                // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
                sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));
                sliceConfig.remove(Constant.CONN_MARK);
                Configuration tempSlice;

                // 已在之前进行了扩展和"处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

                String splitPk = originalConfig.getString(Key.SPLIT_PK, null);


                //最终切分份数不一定等于 eachTableShouldSplittedNumber
                boolean needSplitTable = eachTableShouldSplittedNumber > 1
                        && StringUtils.isNotBlank(splitPk);
                if (needSplitTable) {
                    if (tables.size() == 1) {
                        //原来:如果是单表的，主键切分num=num*2+1
                        // splitPk is null这类的情况的数据量本身就比真实数据量少很多, 和channel大小比率关系时，不建议考虑
                        //eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 2 + 1;// 不应该加1导致长尾

                        //考虑其他比率数字?(splitPk is null, 忽略此长尾)
                        //eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 5;

                        //为避免导入hive小文件 默认基数为5，可以通过 splitFactor 配置基数
                        // 最终task数为(channel/tableNum)向上取整*splitFactor
                        Integer splitFactor = originalConfig.getInt(Key.SPLIT_FACTOR, Constant.SPLIT_FACTOR);
                        eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * splitFactor;
                    }
                    // 尝试对每个表，切分为eachTableShouldSplittedNumber 份
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);

                        List<Configuration> splittedSlices = SingleTableSplitUtil
                                .splitSingleTable(tempSlice, eachTableShouldSplittedNumber);

                        splittedConfigs.addAll(splittedSlices);
                    }
                } else {
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);
                        String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, table, column);
                        tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, table, where));
                        splittedConfigs.add(tempSlice);
                    }
                }
            }
            return splittedConfigs;
        }
    }

    private static int calculateEachTableShouldSplittedNumber(int adviceNumber,
                                                              int tableNumber) {
        double tempNum = 1.0 * adviceNumber / tableNumber;

        return (int) Math.ceil(tempNum);
    }


    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private Configuration readerSliceConfig;
        private Configuration writerSliceConfig;
        private String username;
        private String password;
        private String jdbcUrl;
        private String basicMsg;
        private String table;
        private JdbcUrlParser.JdbcInfo jdbcInfo;
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        private String jobId;



        // 用于实时同步的控制变量
        private EmbeddedEngine engine;
        private ExecutorService executor;
        private volatile boolean stopFlag = false;

        // 列配置和元数据
        private List<String> configuredColumns;  // 配置的列列表（按顺序）
        private Map<String, Integer> columnJdbcTypes;  // 列名 -> JDBC类型映射
        private Map<String, String> columnTypeNames;  // 列名 -> 类型名称映射

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.username = readerSliceConfig.getString(Key.USERNAME);
            this.password = readerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);
            this.table = readerSliceConfig.getString(Key.TABLE);
            this.jobId = readerSliceConfig.getString(Key.JOB_ID);
            this.table = readerSliceConfig.getString(Key.TABLE);

            this.jdbcInfo = JdbcUrlParser.parseSqlServerJdbcUrl(jdbcUrl);

            basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);

            // 获取配置的列列表
            List<String> columnList = readerSliceConfig.getList(Key.COLUMN_LIST, String.class);
            if (columnList == null || columnList.isEmpty()) {
                // 如果没有COLUMN_LIST，尝试从COLUMN获取（可能是逗号分隔的字符串）
                String columnStr = readerSliceConfig.getString(Key.COLUMN);
                if (StringUtils.isNotBlank(columnStr) && !"*".equals(columnStr)) {
                    columnList = new ArrayList<>();
                    String[] columns = columnStr.split(",");
                    for (String col : columns) {
                        columnList.add(col.trim());
                    }
                } else {
                    // 如果配置了*，需要查询数据库获取所有列
                    columnList = DBUtil.getTableColumns(DATABASE_TYPE, jdbcUrl, username, password, table);
                }
            }
            this.configuredColumns = columnList;

            // 获取列的JDBC类型元数据
            if (!configuredColumns.isEmpty()) {
                try {
                    Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcUrl, username, password);
                    try {
                        String columnStr = StringUtils.join(configuredColumns, ",");
                        Triple<List<String>, List<Integer>, List<String>> columnMetaData =
                                DBUtil.getColumnMetaData(conn, table, columnStr);

                        this.columnJdbcTypes = new HashMap<>();
                        this.columnTypeNames = new HashMap<>();

                        List<String> columnNames = columnMetaData.getLeft();
                        List<Integer> jdbcTypes = columnMetaData.getMiddle();
                        List<String> typeNames = columnMetaData.getRight();

                        for (int i = 0; i < columnNames.size(); i++) {
                            String colName = columnNames.get(i);
                            columnJdbcTypes.put(colName.toLowerCase(), jdbcTypes.get(i));
                            columnTypeNames.put(colName.toLowerCase(), typeNames.get(i));
                        }

                        LOG.info("Loaded column metadata for table [{}], columns: [{}]",
                                table, StringUtils.join(configuredColumns, ","));
                    } finally {
                        DBUtil.closeDBResources(null, null, conn);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to load column metadata, will use Debezium schema type instead", e);
                    // 如果获取失败，columnJdbcTypes 为 null，后续会使用 Debezium schema type
                }
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            TaskPluginCollector pluginCollector = super.getTaskPluginCollector();

            LOG.info("Begin to read record \n] {}.", basicMsg);

            ch.qos.logback.classic.Logger debeziumLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("io.debezium");
            debeziumLogger.setLevel(Level.WARN);

            ch.qos.logback.classic.Logger kafkaLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("org.apache.kafka");
            kafkaLogger.setLevel(Level.WARN);


            if (StringUtils.isBlank(jobId) ) {
                LOG.info("[Debezium] jobId或者slotName未指定jobId = {} 启动失败",jobId);
                return;
            }

            LOG.info("[Debezium]启动 jobId = {} ",jobId);

            // 检查并创建 ./pg/ 目录
            File pgDir = new File("./sqlserver");
            if (!pgDir.exists()) {
                boolean created = pgDir.mkdirs();
                if (created) {
                    LOG.info("Created directory: ./sqlserver/");
                } else {
                    LOG.warn("Failed to create directory: ./sqlserver/, will try to continue anyway");
                }
            } else {
                LOG.debug("Directory ./sqlserver/ already exists");
            }

            io.debezium.config.Configuration config = io.debezium.config.Configuration.create()
                    .with("name", "sqlserver-batch-connector-"+jobId)
                    .with("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")

                    // SQL Server 连接配置
                    .with("database.hostname", jdbcInfo.host)
                    .with("database.port", jdbcInfo.port)
                    .with("database.user", username)
                    .with("database.password", password)
                    .with("database.dbname", jdbcInfo.database)

                    // Server 信息（必需）
                    .with("database.server.id", jobId)
                    .with("database.server.name", "sqlserver-server-" + jobId)

                    // 监听范围（格式：schema.table）
                    .with("table.include.list", table)


                    .with("snapshot.mode", "initial")

                    // SQL Server 特定配置
                    // 如果数据库未启用 CDC，可以设置为 "schema_only" 或 "schema_only_recovery"
                    // .with("snapshot.mode", "schema_only")

                    // offset 存储配置
                    .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                    .with("offset.storage.file.filename", "./sqlserver/" + jobId + "offsets.dat")
                    .with("offset.flush.interval.ms", "1000")

                    // schema history 配置
                    //.with("database.history", "io.debezium.relational.history.MemoryDatabaseHistory")
                    // 或者使用文件存储
                     .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                     .with("database.history.file.filename", "./sqlserver/" + jobId + "dbhistory.dat")

                    .with("tombstones.on.delete", "false")
                    .build();

            System.out.println(config.toString());

            this.engine = EmbeddedEngine.create()
                    .using(config)
                    .notifying((records, committer) -> {
                        try {
                            // 检查停止标志
                            if (stopFlag) {
                                return;
                            }

                            for (SourceRecord record : records) {

                                if (record.value() == null) {
                                    // tombstone / heartbeat
                                    committer.markProcessed(record);
                                    continue;
                                }

                                Struct value = (Struct) record.value();

                                // 3. 关键：只处理 Debezium CDC Envelope
                                if (!Envelope.isEnvelopeSchema(value.schema())) {
                                    // 非 CDC 事件（schema、metadata 等），直接忽略
                                    committer.markProcessed(record);
                                    continue;
                                }

                                // ✅ 1. 获取操作类型
                                Envelope.Operation op = Envelope.operationFor(record);

                                // ✅ 2. 获取 before / after Struct
                                Struct before = (Struct) value.get("before");
                                Struct after = (Struct) value.get("after");



                                // 业务处理
                                processEvent(table, op.code(), before, after, recordSender, pluginCollector,
                                        configuredColumns, columnJdbcTypes);

                                // 标记本条已处理
                                committer.markProcessed(record);
                            }

                            // 提交整个 batch
                            committer.markBatchFinished();

                        } catch (Exception e) {
                            if (!stopFlag) {
                                LOG.info("Batch processing failed", e);
                                throw new RuntimeException("Batch processing failed", e);
                            }
                        }
                    })
                    .build();

            // 直接在当前线程执行 Debezium 引擎，直到任务结束或发生异常
            engine.run();
        }

        /**
         * 停止EmbeddedEngine
         */
        private void stopEngine() {
            if (engine != null && engine.isRunning()) {
                try {
                    LOG.info("Stopping EmbeddedEngine...");
                    engine.stop();
                    // 等待engine完全停止
                    int waitCount = 0;
                    while (engine.isRunning() && waitCount < 10) {
                        Thread.sleep(500);
                        waitCount++;
                    }
                } catch (Exception e) {
                    LOG.error("Error stopping engine", e);
                }
            }

            if (executor != null && !executor.isShutdown()) {
                try {
                    executor.shutdown();
                    if (!executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }

        /// 把处理对象发送给channel
        private void processEvent(
                String table,
                String op,
                Struct before,
                Struct after,
                RecordSender recordSender,
                TaskPluginCollector pluginCollector,
                List<String> configuredColumns,
                Map<String, Integer> columnJdbcTypes) {
            Struct payload = null;
            Record record = null;
            switch (op) {
                // create / snapshot 使用 after
                case "c":
                    payload = after;
                    record = JdbcUtil.buildRecord(recordSender,payload,configuredColumns, pluginCollector, columnJdbcTypes);
                    break;
                case "r":
                    break;
                // update 使用最新的 after
                case "u":
                    payload = after;
                    record = JdbcUtil.buildRecord(recordSender,payload,configuredColumns, pluginCollector, columnJdbcTypes);
                    JdbcUtil.buildRecordWithSql(record, 1);
                    break;
                // delete 使用 before
                case "d":
                    payload = before;
                    record = JdbcUtil.buildRecord(recordSender,payload,configuredColumns, pluginCollector, columnJdbcTypes);
                    //String deleteSql = DebezimuSqlUtil.buildDeleteSql(totable, before);
                    JdbcUtil.buildRecordWithSql(record, 2);
                    break;
                default:
                    break;
            }
            if (payload != null) {
                LOG.info("Processing {} record: {}",op,record);
                if (record != null) {
                    recordSender.sendToWriter(record);
                }
            }else {
                LOG.info("Processing record SKIPPED");
            }
        }



        @Override
        public void destroy() {
            // 设置停止标志
            stopFlag = true;
            // 停止engine
            stopEngine();
        }
    }
}
