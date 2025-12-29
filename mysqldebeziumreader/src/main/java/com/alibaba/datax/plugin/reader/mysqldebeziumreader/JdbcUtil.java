package com.alibaba.datax.plugin.reader.mysqldebeziumreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

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
                if (cleanColumnName != null && cleanColumnName.startsWith("`") && cleanColumnName.endsWith("`")) {
                    cleanColumnName = cleanColumnName.substring(1, cleanColumnName.length() - 1);
                }
                Field field = struct.schema().field(cleanColumnName);
                if (field == null) {
                    record.addColumn(new StringColumn((String) null));
                    continue;
                }

                Object value = struct.get(field);
                Schema schema = field.schema();

                Integer jdbcType = columnJdbcTypes.get(cleanColumnName);

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
            java.time.LocalDate date = java.time.LocalDate.ofEpochDay(days);
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

    // TIME / TIME(6)
    private static java.sql.Time parseTime(Object value, Schema schema) {
        if ("io.debezium.time.Time".equals(schema.name())) {
            int millis = (Integer) value;
            java.time.LocalTime time = java.time.LocalTime.ofNanoOfDay((long) millis * 1_000_000);
            return java.sql.Time.valueOf(time);
        }

        if ("io.debezium.time.MicroTime".equals(schema.name())) {
            long micros = (Long) value;
            java.time.LocalTime time = java.time.LocalTime.ofNanoOfDay(micros * 1_000);
            return java.sql.Time.valueOf(time);
        }

        // Kafka Connect TIME 类型
        if ("org.apache.kafka.connect.data.Time".equals(schema.name())) {
            if (value instanceof Number) {
                long millis = ((Number) value).longValue();
                LocalTime time = LocalTime.ofNanoOfDay(millis * 1_000_000);
                return java.sql.Time.valueOf(time);
            } else if (value instanceof java.util.Date) {
                return new java.sql.Time(((java.util.Date) value).getTime());
            } else if (value instanceof String) {
                // 如果是字符串格式的时间，例如 "10:13:05"
                return java.sql.Time.valueOf((String) value);
            } else {
                throw new IllegalArgumentException("Unsupported value type for Time: " + value.getClass());
            }
        }

        // fallback: 尝试按毫秒解析
        long longValue = ((Number) value).longValue();
        java.time.LocalTime time = java.time.LocalTime.ofNanoOfDay(longValue * 1_000_000);
        return java.sql.Time.valueOf(time);
    }

    // TIMESTAMP / DATETIME / DATETIME(6)
    public static Timestamp parseTimestamp(Object value, Schema schema) {
        ZoneOffset dbZoneOffset = ZoneOffset.ofHours(8);
//        if ("io.debezium.time.Timestamp".equals(schema.name())) { // DATETIME
//            long millisUtc = (Long) value;
//            // 按数据库时区解析
//            Instant instant = Instant.ofEpochMilli(millisUtc);
//            LocalDateTime ldt = LocalDateTime.ofInstant(instant, dbZoneOffset);
//            return Timestamp.valueOf(ldt);
//        }
//
//        if ("io.debezium.time.MicroTimestamp".equals(schema.name())) { // DATETIME(6)
//            long micros = (Long) value;
//            long millis = micros / 1_000;
//            int nanos = (int) (micros % 1_000_000) * 1_000; // 微秒转纳秒
//            Instant instant = Instant.ofEpochMilli(millis);
//            LocalDateTime ldt = LocalDateTime.ofInstant(instant, dbZoneOffset).withNano(nanos);
//            return Timestamp.valueOf(ldt);
//        }
        if ("io.debezium.time.Timestamp".equals(schema.name())) {
            long millis = (Long) value;
            // 直接转为 LocalDateTime，不加任何时区
            return Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                    millis / 1000,
                    (int) ((millis % 1000) * 1_000_000),
                    java.time.ZoneOffset.UTC // 用 UTC 保持原样，不影响数值
            ));
        }

        if ("io.debezium.time.MicroTimestamp".equals(schema.name())) { // DATETIME(6)
            long micros = (Long) value;
            long millisPart = micros / 1000;
            int nanosPart = (int) (micros % 1000) * 1_000; // 微秒转纳秒
            return Timestamp.valueOf(LocalDateTime.ofEpochSecond(
                    millisPart / 1000,
                    (int) ((millisPart % 1000) * 1_000_000) + nanosPart,
                    java.time.ZoneOffset.UTC // 保持原样
            ));
        }


//        if ("io.debezium.time.ZonedTimestamp".equals(schema.name())) { // TIMESTAMP
//            // Debezium 本身已经带时区，直接去掉偏移
//            OffsetDateTime odt = OffsetDateTime.parse(value.toString());
//            return Timestamp.valueOf(odt.toLocalDateTime());
//        }

        if ("io.debezium.time.ZonedTimestamp".equals(schema.name())) { // TIMESTAMP
            OffsetDateTime odt = OffsetDateTime.parse(value.toString());
            // 转换到指定时区，例如数据库时区
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
                Instant instant = ((java.util.Date) value).toInstant();//不要时区是对的
                LocalDateTime ldt = LocalDateTime.ofInstant(instant, dbZoneOffset);
                return Timestamp.valueOf(ldt);
            } else if (value instanceof String) {
                LocalDateTime ldt = LocalDateTime.parse((String) value,
                        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
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


    public static String formatStructFieldToStringValue(Object value, Schema schema) {
        if (value == null) {
            return "NULL";
        }

        // ===============================
        // 1️ Debezium 时间逻辑类型（最优先）
        // ===============================
        if (schema != null && schema.name() != null) {
            switch (schema.name()) {

                // DATE -> Integer (epoch day)
                case "io.debezium.time.Date": {
                    int days = (Integer) value;
                    LocalDate date = LocalDate.ofEpochDay(days);
                    return "'" + date + "'";
                }
                // TIME -> Integer (millis of day)
                case "io.debezium.time.Time": {   // TIME(0)
                    int millis = (Integer) value;
                    LocalTime time = LocalTime.ofNanoOfDay(millis * 1_000_000L);
                    return "'" + time.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "'";
                }
                case "io.debezium.time.MicroTime": { // TIME(1~6)
                    long micros = (Long) value;
                    LocalTime time = LocalTime.ofNanoOfDay(micros * 1_000L);
                    return "'" + time.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")) + "'";
                }
                // TIMESTAMP -> ISO-8601 String with offset
                case "io.debezium.time.ZonedTimestamp": {
                    OffsetDateTime odt = OffsetDateTime.parse(value.toString());
                    return "'" + odt.toLocalDateTime()
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "'";
                }
                // DATETIME -> Long (epoch millis, NO timezone)
                case "io.debezium.time.Timestamp": {
                    long millis = (Long) value;
                    LocalDateTime ldt = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(millis),
                            ZoneId.systemDefault()
                    );
                    return "'" + ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "'";
                }
                // DATETIME(1~6) -> Long (epoch micros, NO timezone)
                case "io.debezium.time.MicroTimestamp": {
                    long micros = (Long) value;
                    long seconds = micros / 1_000_000;
                    long nanos = (micros % 1_000_000) * 1_000;

                    LocalDateTime ldt = LocalDateTime.ofEpochSecond(
                            seconds,
                            (int) nanos,
                            ZoneOffset.UTC   // 注意：这里只是数学换算，不是“时区语义”
                    );

                    return "'" + ldt.format(
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    ) + "'";
                }
                case Decimal.LOGICAL_NAME: {
                    BigDecimal bd = (BigDecimal) value;
                    return bd.toPlainString();
                }
            }
        }

        // ===============================
        // 2️ 普通字符串
        // ===============================
        if (value instanceof String) {
            String escaped = ((String) value)
                    .replace("\\", "\\\\")
                    .replace("'", "''");
            return "'" + escaped + "'";
        }

        // ===============================
        // 3️ 数字 / 布尔
        // ===============================
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }

        // ===============================
        // 4️ 兜底
        // ===============================
        String escaped = value.toString()
                .replace("\\", "\\\\")
                .replace("'", "''");
        return "'" + escaped + "'";
    }
}
