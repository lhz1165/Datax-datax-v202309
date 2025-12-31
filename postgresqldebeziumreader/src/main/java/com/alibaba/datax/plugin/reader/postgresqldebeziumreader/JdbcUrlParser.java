package com.alibaba.datax.plugin.reader.postgresqldebeziumreader;

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

    public static JdbcInfo parsePostgresqlJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:postgresql://")) {
            throw new IllegalArgumentException("Invalid PostgreSQL JDBC URL");
        }

        try {
            // 去掉 jdbc:
            String uriPart = jdbcUrl.substring(5); // jdbc:postgresql://...
            java.net.URI uri = java.net.URI.create(uriPart);

            JdbcInfo info = new JdbcInfo();

            info.host = uri.getHost();
            info.port = uri.getPort() == -1 ? 5432 : uri.getPort();

            // path 形如 /dataxweb 或 /dataxweb?currentSchema=public
            String path = uri.getPath();
            if (path != null && path.length() > 1) {
                String[] parts = path.substring(1).split("/");
                info.database = parts[0];
                if (parts.length > 1) {
                    info.schema = parts[1];
                } else {
                    info.schema = "public"; // PostgreSQL 默认 schema
                }
            }

            // 检查 query 参数中的 currentSchema
            String query = uri.getQuery();
            if (query != null) {
                String[] params = query.split("&");
                for (String param : params) {
                    if (param.startsWith("currentSchema=")) {
                        info.schema = param.substring("currentSchema=".length());
                        break;
                    }
                }
            }

            // 如果没有设置 schema，使用默认值
            if (info.schema == null) {
                info.schema = "public";
            }

            return info;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JDBC URL: " + jdbcUrl, e);
        }
    }

    public static void main(String[] args) {
        String jdbcUrl =
                "jdbc:postgresql://192.168.1.111:5432/dataxweb?currentSchema=public";

        JdbcInfo info = parsePostgresqlJdbcUrl(jdbcUrl);
        System.out.println(info);
    }
}

