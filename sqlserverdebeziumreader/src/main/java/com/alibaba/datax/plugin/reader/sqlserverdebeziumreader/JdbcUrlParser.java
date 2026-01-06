package com.alibaba.datax.plugin.reader.sqlserverdebeziumreader;

public class JdbcUrlParser {

    public static class JdbcInfo {
        public String host;
        public int port;
        public String database;
        public String schema;

        @Override
        public String toString() {
            return "JdbcInfo{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    ", database='" + database + '\'' +
                    ", schema='" + schema + '\'' +
                    '}';
        }
    }

    /**
     * 解析 SQL Server JDBC URL，例如：
     * jdbc:sqlserver://localhost:1433;databaseName=test;
     */
    public static JdbcInfo parseSqlServerJdbcUrl(String jdbcUrl) {
        final String prefix = "jdbc:sqlserver://";
        if (jdbcUrl == null || !jdbcUrl.startsWith(prefix)) {
            throw new IllegalArgumentException("Invalid SQLServer JDBC URL");
        }

        try {
            String rest = jdbcUrl.substring(prefix.length());

            // 拆出 host:port 和参数段
            String hostPortPart;
            String paramPart = "";
            int semi = rest.indexOf(';');
            if (semi >= 0) {
                hostPortPart = rest.substring(0, semi);
                paramPart = rest.substring(semi + 1);
            } else {
                hostPortPart = rest;
            }

            JdbcInfo info = new JdbcInfo();

            // 解析 host 与 port（默认 1433）
            String host = hostPortPart;
            int port = 1433;
            int colon = hostPortPart.lastIndexOf(':');
            if (colon > 0) {
                host = hostPortPart.substring(0, colon);
                String portStr = hostPortPart.substring(colon + 1);
                if (!portStr.isEmpty()) {
                    port = Integer.parseInt(portStr);
                }
            }
            info.host = host;
            info.port = port;

            // 解析参数中的 databaseName
            if (!paramPart.isEmpty()) {
                String[] params = paramPart.split(";");
                for (String param : params) {
                    if (param.startsWith("databaseName=")) {
                        info.database = param.substring("databaseName=".length());
                        break;
                    }
                }
            }

            // SQL Server 默认 schema 设为 dbo
            if (info.schema == null) {
                info.schema = "dbo";
            }

            return info;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JDBC URL: " + jdbcUrl, e);
        }
    }

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:sqlserver://localhost:1433;databaseName=test;";

        JdbcInfo info = parseSqlServerJdbcUrl(jdbcUrl);
        System.out.println(info);
    }
}

