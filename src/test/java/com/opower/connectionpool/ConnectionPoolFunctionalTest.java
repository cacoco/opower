package com.opower.connectionpool;

import org.testng.Assert;
import org.testng.annotations.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

@Test(groups = "functional")
public class ConnectionPoolFunctionalTest implements BaseTest {
    // NOTE: This is a functional test and requires an actual postgresql database, I had originally added
    // a profile in the pom.xml for running these via Maven, but thought that it's more useful from inside
    // the IDE.
    private static final Logger LOG = Logger.getLogger(ConnectionPoolFunctionalTest.class.getSimpleName());

    private ConnectionPoolImpl pool;

    @BeforeClass(groups = "functional")
    public void before() throws Exception {
        pool = new ConnectionPoolImpl(driver, url, username, password, maxActive, maxIdle, maxWait, SQL);
        LOG.log(Level.INFO, "Created new pool.");
    }

    @AfterClass(groups = "functional", alwaysRun = true )
    public void after() throws Exception {
        if (pool != null) {
            pool.close(true);
            LOG.log(Level.INFO, "Closed pool.");
        }
    }

    @Test
    public void testBorrowRelease() throws Exception {
        Connection connection = pool.getConnection();
        LOG.log(Level.INFO, "Obtained connection.");
        Assert.assertNotNull(connection);
        int size = pool.getSize(), busy = pool.getBusySize(), idle = pool.getIdleSize();
        Assert.assertTrue(size >= 1);
        Assert.assertTrue(busy >= idle);

        pool.releaseConnection(connection);
        LOG.log(Level.INFO, "Released connection.");
        size = pool.getSize(); busy = pool.getBusySize(); idle = pool.getIdleSize();
        Assert.assertTrue(size >= 1);
        Assert.assertTrue(idle >= busy);
    }

    @Test
    public void testBorrowReleaseBorrow() throws Exception {
        Connection connection = pool.getConnection();
        LOG.log(Level.INFO, "Obtained first connection.");
        Assert.assertNotNull(connection);
        int size = pool.getSize(), busy = pool.getBusySize(), idle = pool.getIdleSize();
        Assert.assertTrue(size >= 1);
        Assert.assertTrue(busy >= idle);
        
        pool.releaseConnection(connection);
        LOG.log(Level.INFO, "Released first connection.");
        size = pool.getSize(); busy = pool.getBusySize(); idle = pool.getIdleSize();
        Assert.assertTrue(size >= 1);
        Assert.assertTrue(idle >= busy);
        
        connection = pool.getConnection();
        LOG.log(Level.INFO, "Obtained second connection.");
        Assert.assertNotNull(connection);
    }

    // need to run this AFTER the other tests
    @Test(dependsOnMethods = {"testBorrowRelease", "testBorrowReleaseBorrow"})
    public void testBorrowAsynchronously() throws Exception {
        final List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
        for (int i = 0; i < 100; i++) {
            long wait = Math.round(Math.random() * 2000);
            tasks.add(new Task(pool, wait));
        }

        final ExecutorService executorService = Executors.newFixedThreadPool(25);
        executorService.invokeAll(tasks);
    }

    static class Task implements Callable<Object> {
        private ConnectionPool pool;
        private long wait;

        Task(ConnectionPool pool, long wait) {
            this.pool = pool;
            this.wait = wait;
        }

        public Object call() throws Exception {
            try {
                final Connection connection = pool.getConnection();
                Thread.sleep(wait);
                pool.releaseConnection(connection);
                return true;
            } catch (SQLException e) {
                LOG.log(Level.INFO, e.getMessage());
                return false;
            }
        }
    }
}
