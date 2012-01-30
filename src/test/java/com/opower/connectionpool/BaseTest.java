package com.opower.connectionpool;

public interface BaseTest {

    String driver = "org.postgresql.Driver";
    String url = "jdbc:postgresql://localhost/test";
    String username = System.getenv("USER");
    String password = "";
    String SQL = "select 1 from user";

    int maxActive = 5;
    int maxIdle = 5;
    int maxWait = 2000; // 2 seconds
}
