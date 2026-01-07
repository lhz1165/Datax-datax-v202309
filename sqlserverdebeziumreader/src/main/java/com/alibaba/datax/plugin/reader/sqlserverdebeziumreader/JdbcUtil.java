package com.alibaba.datax.plugin.reader.sqlserverdebeziumreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcUtil {

    public static void buildRecordWithSql(Record record, int type) {

        if (record == null) {
            return ;
        }
        // 1更新 2删除
        Map<String, String> meta = record.getMeta();
        if (meta == null) {
            record.setMeta(new HashMap<>());
        }
        record.getMeta().put("debezimuop", String.valueOf(type));
        //record.getMeta().put("debezimusql", sql);
    }


    public static Record buildRecord(
            RecordSender recordSender,
            Struct struct,
            List<String> configuredColumns,
            TaskPluginCollector taskPluginCollector,
            Map<String, Integer> columnJdbcTypes) {

        Record record = recordSender.createRecord();

        try {
            for (String columnName : configuredColumns) {

                String cleanColumnName = columnName;
                // SqlServer 使用[]，但配置中可能没有[]
                if (cleanColumnName != null && cleanColumnName.startsWith("[") && cleanColumnName.endsWith("]")) {
                    cleanColumnName = cleanColumnName.substring(1, cleanColumnName.length() - 1);
                }
                Field field = struct.schema().field(cleanColumnName);
                if (field == null) {
                    record.addColumn(new StringColumn((String) null));
                    continue;
                }

                Object value = struct.get(field);
                Schema schema = field.schema();

                Integer jdbcType = columnJdbcTypes.get(cleanColumnName.toLowerCase());

                if (value == null) {
                    record.addColumn(new StringColumn(null));
                    continue;
                }

                switch (jdbcType) {

                    /* ===================== 字符串 ===================== */
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CLOB:
                    case Types.NCLOB:
                        record.addColumn(new StringColumn(value.toString()));
                        break;

                    /* ===================== 整数 ===================== */
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        record.addColumn(new LongColumn(((Number) value).longValue()));
                        break;

                    /* ===================== DECIMAL / NUMERIC ===================== */
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        if (Decimal.LOGICAL_NAME.equals(schema.name())) {
                            BigDecimal bd = (BigDecimal) value;
                            record.addColumn(new DoubleColumn(bd.doubleValue()));
                        } else {
                            record.addColumn(new DoubleColumn(value.toString()));
                        }
                        break;

                    /* ===================== 浮点 ===================== */
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        record.addColumn(new DoubleColumn(((Number) value).doubleValue()));
                        break;

                    /* ===================== TIME ===================== */
                    case Types.TIME:
                        java.util.Date date = parseTime(value, schema);
                        record.addColumn(new DateColumn(date));
                        break;

                    /* ===================== DATE ===================== */
                    case Types.DATE:
                        record.addColumn(new DateColumn(parseDate(value, schema)));
                        break;

                    /* ===================== TIMESTAMP ===================== */
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        record.addColumn(new DateColumn(parseTimestamp(value, schema)));
                        break;

                    /* ===================== 二进制 ===================== */
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        record.addColumn(new BytesColumn((byte[]) value));
                        break;

                    /* ===================== BOOLEAN / BIT ===================== */
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.addColumn(new BoolColumn(parseBoolean(value)));
                        break;

                    default:
                        throw DataXException.asDataXException(
                                DBUtilErrorCode.UNSUPPORTED_TYPE,
                                String.format(
                                        "DataX 不支持该字段类型. 字段名:[%s], JDBC类型:[%s], Debezium类型:[%s]",
                                        cleanColumnName,
                                        jdbcType,
                                        schema.name()
                                )
                        );
                }
            }
        } catch (Exception e) {
            taskPluginCollector.collectDirtyRecord(record, e);
            if (e instanceof DataXException) {
                throw (DataXException) e;
            }
        }

        return record;
    }

    // DATE
    private static java.sql.Date parseDate(Object value, Schema schema) {
        if ("io.debezium.time.Date".equals(schema.name())) {
            int days = (Integer) value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return java.sql.Date.valueOf(date);
        }

        // Kafka Connect DATE 类型
        if ("org.apache.kafka.connect.data.Date".equals(schema.name())) {
            if (value instanceof Number) {
                long epochDay = ((Number) value).longValue();
                return java.sql.Date.valueOf(LocalDate.ofEpochDay(epochDay));
            } else if (value instanceof java.util.Date) {
                return new java.sql.Date(((java.util.Date) value).getTime());
            } else if (value instanceof String) {
                return java.sql.Date.valueOf((String) value);
            }
        }

        return java.sql.Date.valueOf(value.toString());
    }

    // TIME (含精度 0~7)
    private static Time parseTime(Object value, Schema schema) {
        String name = schema.name();
        if ("io.debezium.time.Time".equals(name)) { // millis of day
            int millis = (Integer) value;
            LocalTime time = LocalTime.ofNanoOfDay((long) millis * 1_000_000);
            return Time.valueOf(time);
        }

        if ("io.debezium.time.MicroTime".equals(name)) { // micros of day
            long micros = (Long) value;
            LocalTime time = LocalTime.ofNanoOfDay(micros * 1_000);
            return Time.valueOf(time);
        }

        if ("io.debezium.time.NanoTime".equals(name)) { // nanos of day
            long nanos = (Long) value;
            LocalTime time = LocalTime.ofNanoOfDay(nanos);
            return Time.valueOf(time);
        }

        // Kafka Connect TIME with timezone (rare in SQL Server)
        if ("io.debezium.time.ZonedTime".equals(name)) {
            String time = (String) value;
            OffsetTime offsetTime = OffsetTime.parse(time);
            ZonedDateTime zdt = offsetTime
                    .atDate(LocalDate.now())
                    .atZoneSameInstant(ZoneId.of("Asia/Shanghai"));
            LocalTime localTime = zdt.toLocalTime();
            return Time.valueOf(localTime);
        }

        // fallback: treat as millis of day
        long longValue = ((Number) value).longValue();
        LocalTime time = LocalTime.ofNanoOfDay(longValue * 1_000_000);
        return Time.valueOf(time);
    }

    // TIMESTAMP / TIMESTAMP WITH TIME ZONE
    public static Timestamp parseTimestamp(Object value, Schema schema) {
        ZoneOffset dbZoneOffset = ZoneOffset.ofHours(8);
        
        if ("io.debezium.time.Timestamp".equals(schema.name())) { // millis epoch
            long millis = (Long) value;
            return Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                    millis / 1000,
                    (int) ((millis % 1000) * 1_000_000),
                    ZoneOffset.UTC
            ));
        }

        if ("io.debezium.time.MicroTimestamp".equals(schema.name())) { // micros epoch
            long micros = (Long) value;
            long millisPart = micros / 1000;
            int nanosPart = (int) (micros % 1000) * 1_000; // 微秒转纳秒
            return Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                    millisPart / 1000,
                    (int) ((millisPart % 1000) * 1_000_000) + nanosPart,
                    ZoneOffset.UTC
            ));
        }

        if ("io.debezium.time.NanoTimestamp".equals(schema.name())) { // nanos epoch
            long nanos = (Long) value;
            long seconds = nanos / 1_000_000_000L;
            int nanoAdj = (int) (nanos % 1_000_000_000L);
            return Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                    seconds,
                    nanoAdj,
                    ZoneOffset.UTC
            ));
        }

        // TIMESTAMP WITH TIME ZONE（如有）
        if ("io.debezium.time.ZonedTimestamp".equals(schema.name())) {
            OffsetDateTime odt = OffsetDateTime.parse(value.toString());
            LocalDateTime ldt = odt.atZoneSameInstant(dbZoneOffset).toLocalDateTime();
            return Timestamp.valueOf(ldt);
        }

        // Kafka Connect 原生 Timestamp 类型
        if ("org.apache.kafka.connect.data.Timestamp".equals(schema.name())) {
            if (value instanceof Number) {
                long millis = ((Number) value).longValue();
                Instant instant = Instant.ofEpochMilli(millis);
                LocalDateTime ldt = LocalDateTime.ofInstant(instant, dbZoneOffset);
                return Timestamp.valueOf(ldt);
            } else if (value instanceof java.util.Date) {
                Instant instant = ((java.util.Date) value).toInstant();
                LocalDateTime ldt = LocalDateTime.ofInstant(instant, dbZoneOffset);
                return Timestamp.valueOf(ldt);
            } else if (value instanceof String) {
                LocalDateTime ldt = LocalDateTime.parse((String) value,
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                return Timestamp.valueOf(ldt);
            }
        }

        // fallback
        return new Timestamp(((Number) value).longValue());
    }


    private static boolean parseBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        if (value instanceof byte[]) {
            return ((byte[]) value)[0] != 0;
        }
        return Boolean.parseBoolean(value.toString());
    }

}

