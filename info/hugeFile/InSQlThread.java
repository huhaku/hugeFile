package info.ggdog.hugeFile;

import info.ggdog.hugeFile.util.DruidUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;

public class InSQlThread implements Runnable {
    private LinkedList<String> sonStringList = null;

    public InSQlThread(LinkedList<String> sonStringList) {
        this.sonStringList = sonStringList;
    }

    public InSQlThread() {
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + "start");
        long threadStart = System.currentTimeMillis();
        Connection conn = DruidUtils.getConnection();
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String temp1 = null;
        int i = 0;
        PreparedStatement ps = null;
        try {
            // 将短数据拼接成一条长SQL一次提交
            String sql = "INSERT INTO `components_warehouse` " +
                    "VALUES(NULL,?,?)," +
                    "(NULL,?,?)," +
                    "(NULL,?,?)," +
                    "(NULL,?,?)," +
                    "(NULL,?,?)," +
                    "(NULL,?,?)," +
                    "(NULL,?,?)," +
                    "(NULL,?,?)," +
                    "(NULL,?,?)," +
                    "(NULL,?,?);";
            ps = conn.prepareStatement(sql);
            int size = sonStringList.size();
            for (int i1 = 0; i1 < size; i1 += 10) {
                for (int i2 = 1; i2 <= 20; i2 += 2) {
                    temp1 = sonStringList.poll();
                    String[] result = temp1.split("----");
                    ps.setString(i2, result[0]);
                    ps.setString(i2 + 1, result[1]);
                }
                ps.executeUpdate();
                i++;
                if (i % 100 == 0) { // 每1000条数据拆分成一次事务提交
                    conn.commit();
                    conn.setAutoCommit(false);
                }
            }
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DruidUtils.close(conn, ps);
        }
        Long threadEnd = System.currentTimeMillis();
        System.out.println(Thread.currentThread().getName() + ":" + (threadEnd - threadStart) + "ms");
    }
}
