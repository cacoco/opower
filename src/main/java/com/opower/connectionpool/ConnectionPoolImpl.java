package com.opower.connectionpool;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionPoolImpl implements ConnectionPool {
    private static final Logger LOG = Logger.getLogger(ConnectionPoolImpl.class.getName());

    private BlockingQueue<ConnectionWrapper> busy; // currently in-use
    private BlockingQueue<ConnectionWrapper> idle; // currently waiting to be used

    private int maxActive;  // maximum number of active connections
    private int maxIdle;    // maximum number of idle connections
    private long maxWait;   // how long to wait for a connection

    private Driver driver;
    private String driverClassName;
    private String driver_url;
    private String username;
    private String password;
    private String validateSQL;

    // current size, used instead of iterating over the queue every time.
    private AtomicInteger size = new AtomicInteger(0);
    // how many are currently waiting for a connection.
    private AtomicInteger waitCount = new AtomicInteger(0);

    // allow a way to shut down this pool
    private volatile boolean closed = false;

    // all of this configuration could be pulled out into an obj
    public ConnectionPoolImpl(final String driverClassName,
                              final String driver_url,
                              final String username,
                              final String password,
                              final int maxActive,
                              final int maxIdle,
                              final long maxWait,
                              final String validateSQL) {
        this.driverClassName = driverClassName;
        this.driver_url = driver_url;
        this.username = username;
        this.password = password;
        this.maxActive = maxActive;
        this.maxIdle = maxIdle;
        this.maxWait = maxWait;
        this.validateSQL = validateSQL;
        
        busy = new ArrayBlockingQueue<ConnectionWrapper>(maxActive, true);
        idle = new ArrayBlockingQueue<ConnectionWrapper>(maxActive, true);
    }

    public Connection getConnection() throws SQLException {
        final ConnectionWrapper connection = borrow(-1);
        return connection.getConnection(); // SHOULD never return null.
    }
    
    protected ConnectionWrapper borrow(long wait) throws SQLException {
        if (isClosed()) { throw new SQLException("This pool is CLOSED. Please swim somewhere else."); }
        long now = System.currentTimeMillis();
        ConnectionWrapper connection = idle.poll();

        while (true) {
            if (connection != null) {
                ConnectionWrapper result = defrostConnection(connection, now);
                if (result != null) { return result; }
            }

            if (size.get() < maxActive) {
                if (size.addAndGet(1) > maxActive) {
                    size.decrementAndGet();
                } else {
                    ConnectionWrapper result = createConnection(now);
                    if (result != null) { return result; }
                }
            }
            // calculate a time to wait for this iteration
            long maxWait = wait;
            if (wait == -1) { maxWait = this.maxWait <= 0 ? Long.MAX_VALUE : this.maxWait; }
            long timeToWait = Math.max(0, maxWait - (System.currentTimeMillis() - now));
            waitCount.incrementAndGet();

            try {
                // try to get an existing idle connection
                connection = idle.poll(timeToWait, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.interrupted(); // ack, clear the flag, and hurl SQLException
                throw new SQLException(e.getMessage(), e);
            } finally {
                // either we've hurled of we have a connection, so decrement the waiting
                waitCount.decrementAndGet();
            }
            if (maxWait == 0 && connection == null) { //no wait, return one if we have one
                // TODO: should I print out which thread we're in?
                throw new SQLException(String.format("No connections available: [%s] connections in use.",
                        busy.size()));
            }
            // we didn't get a connection, timed out?
            if (connection == null) {
                if ((System.currentTimeMillis() - now) >= maxWait) {
                    // TODO: add thread currently executing
                    throw new SQLException(String.format("Timed out trying to retrieve connection from pool. Waited [%dms], waiting count: [%s], busy: [%s]",
                            maxWait,
                            waitCount.get(),
                            busy.size()));
                } else {
                    // well, do it again
                    continue;
                }
            }
        }
    }

    public void releaseConnection(Connection conn) throws SQLException {
        if (isClosed()) {
            // if the connection pool is closed
            // close the connection instead of returning it to the pool
            release(wrap(conn));
            return;
        }

        if (conn != null) {
            ConnectionWrapper connection = wrap(conn);
            try {
                connection.lock();
                if (busy.remove(connection)) {
                    if (!shouldClose(connection)) {
                        connection.setTimestamp(System.currentTimeMillis());
                        if (idle.size() >= this.maxIdle ||
                                !idle.offer(connection)) {
                            // either the idle size is larger than we're allowed or the idle queue is full/busy
                            release(connection);
                        }
                    } else {
                        // otherwise we should release/close the connection
                        release(connection);
                    }
                } else {
                    // else we couldn't remove from the busy queue, release/close.
                    release(connection);
                }
            } finally {
                connection.unlock();
            }
        }
    }

    protected void release(ConnectionWrapper connection) {
        if (connection == null) { return; }
        try {
            connection.lock();
            if (connection.release()) {
                // decrement size
                size.addAndGet(-1);
            }
        } finally {
            connection.unlock();
        }
        // we've reduced the number of connections but we could have threads
        // waiting that will never be notified, so add to the idle queue
        if (waitCount.get() > 0) {
            idle.offer(create(true));
        }
    }

    public void close(boolean force) {
        if (this.closed) { return; }
        this.closed = true;

        // release all connections
        BlockingQueue<ConnectionWrapper> pool = idle.size() > 0 ? idle : (force ? busy : idle);
        while (pool.size() > 0) {
            try {
                ConnectionWrapper connection = pool.poll(1000, TimeUnit.MILLISECONDS);
                // close it and get the next one if it's available
                while (connection != null) {
                    // close the connection
                    release(connection);
                    connection = pool.poll(1000, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
            if (pool.size() == 0 && force && pool != busy) { pool = busy; }
        }
    }

    public int getSize() {
        return size.get();
    }

    public boolean isClosed() {
        return closed;
    }

    protected void setClosed(boolean closed) {
        this.closed = closed;
    }

    protected void setDriver(Driver driver) {
        this.driver = driver;
    }

    protected ConnectionWrapper createConnection(final long now) throws SQLException {
        ConnectionWrapper connection = create(false);

        connection.lock();
        try {
            //connect and validate the connection
            connection.connect();
            if (connection.validate()) {
                connection.setTimestamp(now);
                if (!busy.add(connection)) {
                    LOG.log(Level.WARNING, "Cannot add connection to in-use queue.");
                    // TODO: die here?
                }
                return connection;
            } else {
                // couldn't validate, need to disconnect and clean up
                release(connection);
            }
        } catch (Exception e) {
            release(connection);
            throw new SQLException(e.getMessage(), e);
        } finally {
            connection.unlock();
        }
        return null;
    }

    protected ConnectionWrapper create(boolean incrementCounter) {
        if (incrementCounter) { this.size.incrementAndGet(); }
        ConnectionWrapper connection = new ConnectionWrapper(
                this.driverClassName,
                this.driver_url,
                this.username,
                this.password,
                this.validateSQL);
        if (driver != null) { connection.setDriver(driver); }
        return connection;
    }
    
    protected ConnectionWrapper wrap(final Connection connection) throws SQLException {
        return new ConnectionWrapper(
                this.driverClassName,
                this.driver_url,
                this.username,
                this.password,
                this.validateSQL,
                connection);
    }

    protected ConnectionWrapper defrostConnection(ConnectionWrapper connection, long now) throws SQLException {
        try {
            connection.lock();
            if (connection.isReleased()) { return null; }

            if (!connection.isDiscarded() &&
                    !connection.isInitialized()) {
                // not discarded and not initialized, try to connect
                connection.connect();
                return connection;
            }

            // was discarded or previously initialized, attempt to reconnect
            try {
                connection.reconnect();
                if (connection.validate()) {
                    // set the accessed timestamp
                    connection.setTimestamp(now);
                    if (!busy.add(connection)) {
                        LOG.log(Level.WARNING, "Cannot add connection to in-use queue.");
                    }
                    return connection;
                } else {
                    // validation failed.
                    release(connection);
                    throw new SQLException("Failed to validate a newly established connection.");
                }
            } catch (Exception e) {
                release(connection);
                throw new SQLException(e.getMessage(), e);
            }
        } finally {
            connection.unlock();
        }
    }

    // if we're returning to the pool -- should we close?
    protected boolean shouldClose(ConnectionWrapper connection) {
        return connection.isDiscarded() || isClosed() || !connection.validate();
    }

    protected int getIdleSize() {
        return idle.size();
    }

    protected int getBusySize() {
        return busy.size();
    }
}
