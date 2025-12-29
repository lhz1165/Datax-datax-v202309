package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.List;

public class DebezimuSqlUtil {

    /**
     * 构建 UPDATE SQL 语句
     * UPDATE table SET col1=value1, col2=value2, ... WHERE col1=value1 AND col2=value2 AND ...
     */
    public static String buildUpdateSql(String table, Triple<List<String>, List<Integer>, List<String>> resultSetMetaData,
                                        Record record, DataBaseType dataBaseType, boolean emptyAsNull) {
        List<String> columnNames = resultSetMetaData.getLeft();
        List<Integer> columnTypes = resultSetMetaData.getMiddle();
        List<String> typeNames = resultSetMetaData.getRight();

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(table).append(" SET ");

        // 构建 SET 子句
        boolean first = true;
        for (int i = 0; i < columnNames.size(); i++) {
            if (!first) {
                sql.append(", ");
            } else {
                first = false;
            }
            String columnName = quoteColumnName(columnNames.get(i));
            String value = formatColumnValue(record.getColumn(i), columnTypes.get(i),
                    typeNames.get(i), dataBaseType, emptyAsNull);
            sql.append(columnName).append("=").append(value);
        }

        // 构建 WHERE 子句
        // 优先使用 id 作为条件（如果存在），否则使用所有列
        sql.append(" WHERE ");
        // 查找名为 id 的列索引（支持 `id` 写法）
        int idIndex = -1;
        for (int i = 0; i < columnNames.size(); i++) {
            String rawName = columnNames.get(i);
            String cleanName = trimBackticks(rawName);
            if ("id".equalsIgnoreCase(cleanName)) {
                idIndex = i;
                break;
            }
        }

        if (idIndex >= 0) {
            // 仅使用 id 作为 WHERE 条件
            String columnName = quoteColumnName(columnNames.get(idIndex));
            String value = formatColumnValue(record.getColumn(idIndex), columnTypes.get(idIndex),
                    typeNames.get(idIndex), dataBaseType, emptyAsNull);
            sql.append(columnName).append("=").append(value);
        } else {
            // 使用所有列作为条件
            first = true;
            for (int i = 0; i < columnNames.size(); i++) {
                if (!first) {
                    sql.append(" AND ");
                } else {
                    first = false;
                }
                String columnName = quoteColumnName(columnNames.get(i));
                String value = formatColumnValue(record.getColumn(i), columnTypes.get(i),
                        typeNames.get(i), dataBaseType, emptyAsNull);
                sql.append(columnName).append("=").append(value);
            }
        }

        return sql.toString();
    }

    /**
     * 构建 DELETE SQL 语句
     * DELETE FROM table WHERE col1=value1 AND col2=value2 AND ...
     */
    public static String buildDeleteSql(String table, Triple<List<String>, List<Integer>, List<String>> resultSetMetaData,
                                        Record record, DataBaseType dataBaseType, boolean emptyAsNull) {
        List<String> columnNames = resultSetMetaData.getLeft();
        List<Integer> columnTypes = resultSetMetaData.getMiddle();
        List<String> typeNames = resultSetMetaData.getRight();

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(table).append(" WHERE ");

        // 构建 WHERE 子句
        // 优先使用 id 作为条件（如果存在），否则使用所有列
        boolean first = true;
        // 查找名为 id 的列索引（支持 `id` 写法）
        int idIndex = -1;
        for (int i = 0; i < columnNames.size(); i++) {
            String rawName = columnNames.get(i);
            String cleanName = trimBackticks(rawName);
            if ("id".equalsIgnoreCase(cleanName)) {
                idIndex = i;
                break;
            }
        }

        if (idIndex >= 0) {
            String columnName = quoteColumnName(columnNames.get(idIndex));
            String value = formatColumnValue(record.getColumn(idIndex), columnTypes.get(idIndex),
                    typeNames.get(idIndex), dataBaseType, emptyAsNull);
            sql.append(columnName).append("=").append(value);
        } else {
            for (int i = 0; i < columnNames.size(); i++) {
                if (!first) {
                    sql.append(" AND ");
                } else {
                    first = false;
                }
                String columnName = quoteColumnName(columnNames.get(i));
                String value = formatColumnValue(record.getColumn(i), columnTypes.get(i),
                        typeNames.get(i), dataBaseType, emptyAsNull);
                sql.append(columnName).append("=").append(value);
            }
        }

        return sql.toString();
    }

    /**
     * 构建 SQL 语句（根据操作类型）
     */
    public static String buildSql(String table, Triple<List<String>, List<Integer>, List<String>> resultSetMetaData,
                                  String type, Record record, DataBaseType dataBaseType, boolean emptyAsNull) {
        String sql = null;
        try {
            switch (type) {
                case "1":
                    sql = buildUpdateSql(table, resultSetMetaData, record, dataBaseType, emptyAsNull);
                    break;
                case "2":
                    sql = buildDeleteSql(table, resultSetMetaData, record, dataBaseType, emptyAsNull);
                    break;
                default:
                    break;
            }
        }catch (Exception e){
            return null;
        }
        return sql;
    }

    /**
     * 将 Column 值格式化为 SQL 字符串字面量
     * 参考 fillPreparedStatementColumnType 的逻辑
     */
    private static String formatColumnValue(Column column, int columnSqltype, String typeName,
                                            DataBaseType dataBaseType, boolean emptyAsNull) {
        if (column == null || column.getRawData() == null) {
            return "NULL";
        }

        try {
            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    String strValue = column.asString();
                    if (emptyAsNull && StringUtils.isBlank(strValue)) {
                        return "NULL";
                    }
                    return escapeString(strValue, dataBaseType);

                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    strValue = column.asString();
                    if (emptyAsNull && StringUtils.isBlank(strValue)) {
                        return "NULL";
                    }
                    return strValue;

                //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
                case Types.TINYINT:
                    Long longValue = column.asLong();
                    if (null == longValue) {
                        return "NULL";
                    }
                    return longValue.toString();

                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                case Types.DATE:
                    if (typeName != null && typeName.equalsIgnoreCase("year")) {
                        if (column.asBigInteger() == null) {
                            return "NULL";
                        }
                        return column.asBigInteger().toString();
                    } else {
                        java.util.Date utilDate = column.asDate();
                        if (null == utilDate) {
                            return "NULL";
                        }
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        return escapeString(sdf.format(utilDate), dataBaseType);
                    }

                case Types.TIME:
                    java.util.Date utilDate = column.asDate();
                    if (null == utilDate) {
                        return "NULL";
                    }
                    SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
                    return escapeString(timeFormat.format(utilDate), dataBaseType);

                case Types.TIMESTAMP:
                    utilDate = column.asDate();
                    if (null == utilDate) {
                        return "NULL";
                    }
                    SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    return escapeString(timestampFormat.format(utilDate), dataBaseType);

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    byte[] bytes = column.asBytes();
                    if (null == bytes) {
                        return "NULL";
                    }
                    // 二进制数据转换为十六进制字符串
                    return "0x" + bytesToHex(bytes);

                case Types.BOOLEAN:
                    Boolean boolValue = column.asBoolean();
                    if (null == boolValue) {
                        return "NULL";
                    }
                    return boolValue ? "1" : "0";

                // warn: bit(1) -> Types.BIT 可使用setBoolean
                // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
                case Types.BIT:
                    if (dataBaseType == DataBaseType.MySql) {
                        Boolean bitValue = column.asBoolean();
                        if (null == bitValue) {
                            return "NULL";
                        }
                        return bitValue ? "1" : "0";
                    } else {
                        strValue = column.asString();
                        if (emptyAsNull && StringUtils.isBlank(strValue)) {
                            return "NULL";
                        }
                        return escapeString(strValue, dataBaseType);
                    }
                default:
                    // 对于不支持的类型，尝试转换为字符串
                    strValue = column.asString();
                    if (emptyAsNull && StringUtils.isBlank(strValue)) {
                        return "NULL";
                    }
                    return escapeString(strValue, dataBaseType);
            }
        } catch (Exception e) {
            // 如果转换失败，尝试使用字符串表示
            try {
                String strValue = column.asString();
                if (emptyAsNull && StringUtils.isBlank(strValue)) {
                    return "NULL";
                }
                return escapeString(strValue, dataBaseType);
            } catch (Exception ex) {
                return "NULL";
            }
        }
    }

    /**
     * 转义字符串值，添加单引号
     */
    private static String escapeString(String value, DataBaseType dataBaseType) {
        if (value == null) {
            return "NULL";
        }

        // 转义单引号
        String escaped;
        if (dataBaseType == DataBaseType.MySql || dataBaseType == DataBaseType.Tddl ||
                dataBaseType == DataBaseType.DRDS) {
            escaped = value.replace("'", "''").replace("\\", "\\\\");
        } else if (dataBaseType == DataBaseType.Oracle || dataBaseType == DataBaseType.SQLServer) {
            escaped = value.replace("'", "''");
        } else {
            // 默认转义方式
            escaped = value.replace("'", "''");
        }

        return "'" + escaped + "'";
    }

    /**
     * 去掉列名两侧的反引号（如果有）
     */
    private static String trimBackticks(String name) {
        if (name == null) {
            return null;
        }
        String result = name.trim();
        if (result.length() >= 2 && result.startsWith("`") && result.endsWith("`")) {
            result = result.substring(1, result.length() - 1);
        }
        return result;
    }

    /**
     * 统一将列名用反引号包裹起来：先去掉已有的 ` 再加上 `col`
     */
    private static String quoteColumnName(String name) {
        String clean = trimBackticks(name);
        if (clean == null) {
            return null;
        }
        return "`" + clean + "`";
    }

    /**
     * 将字节数组转换为十六进制字符串
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02X", b));
        }
        return hex.toString();
    }
}
