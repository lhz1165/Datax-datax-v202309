package com.alibaba.datax.plugin.reader.mysqldebeziumreader;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;

public class DebezimuSqlUtil {

    private static ZoneId databaseTimezone = ZoneId.of("Asia/Shanghai");




    /**
     * 构造 UPDATE SQL 语句
     */
    public static String buildUpdateSql(String table, Struct before, Struct after) {
        if (after == null || after.schema() == null) {
            return "-- UPDATE: after struct is null";
        }
        if (before == null || before.schema() == null) {
            return "-- UPDATE: before struct is null";
        }

        StringBuilder setClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();

        // SET 子句：使用 after 的值
        after.schema().fields().forEach(field -> {
            if (setClause.length() > 0) {
                setClause.append(", ");
            }
            String fieldName = field.name();
            Object value = after.get(field);
            setClause.append("`").append(fieldName).append("` = ").append(formatSqlValue(value,field.schema()));
        });

        // WHERE 子句：优先使用 id 字段，如果没有 id 则使用所有字段
        org.apache.kafka.connect.data.Field idField = before.schema().field("id");
        if (idField != null) {
            // 使用 id 字段作为 WHERE 条件
            Object idValue = before.get(idField);
            whereClause.append("`id` = ").append(formatSqlValue(idValue, idField.schema()));
        } else {
            // 如果没有 id 字段，使用所有字段作为 WHERE 条件
            before.schema().fields().forEach(field -> {
                if (whereClause.length() > 0) {
                    whereClause.append(" AND ");
                }
                String fieldName = field.name();
                Object value = before.get(field);
                whereClause.append("`").append(fieldName).append("` = ").append(formatSqlValue(value,field.schema()));
            });
        }

        return String.format("UPDATE `%s` SET %s WHERE %s;", table, setClause, whereClause);
    }

    /**
     * 构造 DELETE SQL 语句
     */
    public static String buildDeleteSql(String table, Struct before) {
        if (before == null || before.schema() == null) {
            return "-- DELETE: before struct is null";
        }

        StringBuilder whereClause = new StringBuilder();

        // 优先使用 id 字段，如果没有 id 则使用所有字段
        org.apache.kafka.connect.data.Field idField = before.schema().field("id");
        if (idField != null) {
            // 使用 id 字段作为 WHERE 条件
            Object idValue = before.get(idField);
            whereClause.append("`id` = ").append(formatSqlValue(idValue, idField.schema()));
        } else {
            // 如果没有 id 字段，使用所有字段作为 WHERE 条件
            before.schema().fields().forEach(field -> {
                if (whereClause.length() > 0) {
                    whereClause.append(" AND ");
                }
                String fieldName = field.name();
                Object value = before.get(field);
                whereClause.append("`").append(fieldName).append("` = ").append(formatSqlValue(value,field.schema()));
            });
        }

        return String.format("DELETE FROM `%s` WHERE %s;", table, whereClause);
    }

    /**
     * 格式化 SQL 值（处理 NULL、字符串转义、时间类型格式化等）
     */
    public static String formatSqlValue(Object value, Schema schema) {
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
                            databaseTimezone
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
