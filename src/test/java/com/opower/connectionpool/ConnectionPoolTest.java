package com.opower.connectionpool;

import org.easymock.EasyMockSupport;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Properties;
import java.util.logging.Logger;

import static org.easymock.EasyMock.*;

@Test
public class ConnectionPoolTest extends EasyMockSupport implements BaseTest {
    private static final Logger LOG = Logger.getLogger(ConnectionPoolTest.class.getSimpleName());
    
    private Driver _driver;
    private Connection _connection;
    private Statement _statement;

    private ConnectionPoolImpl pool;

    @BeforeMethod
    public void before() throws Exception {
        _driver = createMock(Driver.class);
        _connection = createMock(Connection.class);
        _statement = createMock(Statement.class);

        pool = new ConnectionPoolImpl(driver, url, username, password, maxActive, maxIdle, maxWait, SQL);
        pool.setDriver(_driver);

        resetAll();
    }

    @AfterMethod(alwaysRun = true)
    public void after() throws Exception {}

    @Test
    public void testBorrow() throws Exception {
        expect(_driver.connect((String)anyObject(), (Properties)anyObject())).andReturn(_connection);

        // validate()
        expect(_connection.createStatement()).andReturn(_statement);
        expect(_statement.execute((String)anyObject())).andReturn(true);
        _statement.close();

        replayAll();

        ConnectionWrapper connection = pool.borrow(-1);
        Assert.assertNotNull(connection);
        Assert.assertEquals(_connection, connection.getConnection());
        Assert.assertNotNull(connection.getTimestamp());
        Assert.assertEquals(1, pool.getSize());

        verifyAll();
    }

    @Test
    public void testClosedBorrow() throws Exception {

        pool.setClosed(true);
        try {
            pool.getConnection();
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertNotNull(e);
        } finally {
            pool.setClosed(false);
        }
    }

    @Test
    public void testRelease() throws Exception {
        expect(_driver.connect((String)anyObject(), (Properties)anyObject())).andReturn(_connection);

        // validate() called twice, once for borrow and once for release
        expect(_connection.createStatement()).andReturn(_statement).times(2);
        expect(_statement.execute((String)anyObject())).andReturn(true).times(2);
        _statement.close();
        expectLastCall().times(2);

        replayAll();

        int busy = pool.getBusySize(); // size before we get connection
        Connection connection = pool.getConnection();
        Assert.assertNotNull(connection);

        pool.releaseConnection(connection);
        Assert.assertTrue(pool.getBusySize() == busy); // busy should go back to original size
        Assert.assertTrue(pool.getIdleSize() >= 1);

        verifyAll();
    }

    @Test
    public void testReleaseClosed() throws Exception {
        expect(_driver.connect((String)anyObject(), (Properties)anyObject())).andReturn(_connection);

        // validate()
        expect(_connection.createStatement()).andReturn(_statement);
        expect(_statement.execute((String)anyObject())).andReturn(true);
        _statement.close();
        _connection.close(); // connection should get closed as pool is closed.

        replayAll();

        try {
            Connection connection = pool.getConnection();
            Assert.assertNotNull(connection);

            pool.setClosed(true);
            pool.releaseConnection(connection);
        } finally {
            pool.setClosed(false);
        }
    }
    
    @Test
    public void testBorrowReleaseBorrow() throws Exception {
        expect(_driver.connect((String)anyObject(), (Properties)anyObject())).andReturn(_connection).anyTimes();

        // validate()
        expect(_connection.createStatement()).andReturn(_statement).anyTimes();
        expect(_statement.execute((String)anyObject())).andReturn(true).anyTimes();
        _statement.close();
        expectLastCall().anyTimes();

        // disconnect()
        _connection.close();
        
        replayAll();

        int busy = pool.getBusySize();
        Connection connection = pool.getConnection();
        Assert.assertNotNull(connection);

        pool.releaseConnection(connection);
        Assert.assertTrue(pool.getBusySize() == busy);
        Assert.assertTrue(pool.getIdleSize() >= 1);

        connection = pool.getConnection();
        Assert.assertNotNull(connection);

        verifyAll();

    }
    
    @Test
    public void testBorrowTooMany() throws Exception {
        expect(_driver.connect((String)anyObject(), (Properties)anyObject())).andReturn(_connection).anyTimes();

        // validate()
        expect(_connection.createStatement()).andReturn(_statement).anyTimes();
        expect(_statement.execute((String)anyObject())).andReturn(true).anyTimes();
        _statement.close();
        expectLastCall().anyTimes();

        // disconnect()
        _connection.close();
        expectLastCall().anyTimes();

        replayAll();

        LinkedList<Connection> connections = new LinkedList<Connection>();
        for (int i = 0; i < maxActive; i++) {
            connections.add(pool.getConnection());
        }

        Assert.assertEquals(maxActive, pool.getBusySize());
        Assert.assertEquals(maxActive, pool.getSize());
        
        // now try to borrow one more
        try {
            Connection connection = pool.getConnection();
            Assert.fail();
        } catch (SQLException e) {
            // this should blow up after timing out (maxWait)
            Assert.assertNotNull(e);
        }

        // add one connection back
        pool.releaseConnection(connections.pop());
        Assert.assertEquals(1, pool.getIdleSize());
        
        // try to get a connection again, should not fail
        Connection connection = pool.getConnection();
        Assert.assertNotNull(connection);

        verifyAll();
    }
}
