package com.alibaba.datax.plugin.reader.mysqldebeziumreader;

public class JdbcUrlParser {

    public static class JdbcInfo {
        public String host;
        public int port;
        public String database;

        @Override
        public String toString() {
            return "JdbcInfo{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    ", database='" + database + '\'' +
                    '}';
        }
    }

    public static JdbcInfo parseMysqlJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:mysql://")) {
            throw new IllegalArgumentException("Invalid MySQL JDBC URL");
        }

        try {
            // 去掉 jdbc:
            String uriPart = jdbcUrl.substring(5); // jdbc:mysql://...
            java.net.URI uri = java.net.URI.create(uriPart);

            JdbcInfo info = new JdbcInfo();

            info.host = uri.getHost();
            info.port = uri.getPort() == -1 ? 3306 : uri.getPort();

            // path 形如 /dataxweb
            String path = uri.getPath();
            if (path != null && path.length() > 1) {
                info.database = path.substring(1);
            }

            return info;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JDBC URL: " + jdbcUrl, e);
        }
    }

    public static void main(String[] args) {
        String jdbcUrl =
                "jdbc:mysql://192.168.1.111:3306/dataxweb?serverTimezone=Asia/Shanghai&useSSL=false";

        JdbcInfo info = parseMysqlJdbcUrl(jdbcUrl);
        System.out.println(info);
    }
}
