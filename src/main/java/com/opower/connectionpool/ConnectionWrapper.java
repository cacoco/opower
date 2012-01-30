package com.opower.connectionpool;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionWrapper {
    private static final Logger LOG = Logger.getLogger(ConnectionWrapper.class.getName());

    // Lock just for this connection
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(false);

    // The actual database SQL connection
    private volatile Connection connection;

    private Driver driver = null;
    private String driverClassName;
    private String driver_url;
    private String username;
    private String password;
    private String validateSQL;

    // track the time the connection was accessed by the pool
    private volatile long timestamp;

    // has the pool dumped me?
    private volatile boolean discarded = false;
    // am I free?
    private AtomicBoolean released = new AtomicBoolean(false);

    public ConnectionWrapper(final String driverClassName,
                             final String driver_url,
                             final String username,
                             final String password,
                             final String validateSQL) {
        this.driverClassName = driverClassName;
        this.driver_url = driver_url;
        this.username = username;
        this.password = password;
        this.validateSQL = validateSQL;
    }
    
    public ConnectionWrapper(final String driverClassName,
                             final String driver_url,
                             final String username,
                             final String password,
                             final String validateSQL,
                             final Connection connection) throws SQLException {
        this.driverClassName = driverClassName;
        this.driver_url = driver_url;
        this.username = username;
        this.password = password;
        this.validateSQL = validateSQL;

        this.connection = connection;
    }

    public void lock() {
        lock.writeLock().lock();
    }
    
    public void unlock() {
        lock.writeLock().unlock();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isReleased() {
        return released.get();
    }

    public boolean isDiscarded() {
        return discarded;
    }

    public void setDiscarded(boolean discarded) {
        this.discarded = discarded;
    }

    public boolean isInitialized() {
        // if we have a connection => we're initialized
        return connection != null;
    }

    public void reconnect() throws SQLException {
        disconnect();
        connect();
    }

    public void connect() throws SQLException {
        // connect to the db
        if (released.get()) throw new SQLException("Can't re-establish a released connection.");
        if (connection != null) {
            // we already have a connection, need to disconnect it first.
            try {
                this.disconnect();
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Unable to disconnect previous connection.", e);
            }
        }

        // create a new SQLConnection based on the passed driver information
        try {
            if (driver == null) {
                driver = (java.sql.Driver) Class.forName(
                        this.driverClassName,
                        true,
                        ConnectionWrapper.class.getClassLoader()).newInstance();
            }
        } catch (Exception e) {
            final String message = String.format("Unable to create JDBC driver for driver class: [%s]. %s", this.driverClassName, e.getMessage());
            LOG.log(Level.SEVERE, message);
            throw new SQLException(message, e);
        }

        final Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        try {
            connection = driver.connect(driver_url, properties);
            if (connection == null) {
                throw new Exception(String.format("No connection returned for driver: [%s].", this.driverClassName));
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, String.format("Unable to connect to database. %s", e.getMessage()));
            throw new SQLException(e.getMessage(), e);
        }

        this.discarded = false;
    }

    public void disconnect() {
        if (isDiscarded()) { return; }
        setDiscarded(true);
        if (connection != null) {
            // close down shop
            try {
                connection.close();
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Unable to close underlying SQLConnection: " + e.getMessage());
            }
        }
    }

    // validate the underlying connection
    public boolean validate() {
        if (this.isDiscarded()) { return false; }
        if (validateSQL == null || validateSQL.equals("")) { return true;}

        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(validateSQL);
            statement.close();
            return true;
        } catch (Exception e) {
            LOG.log(Level.WARNING, e.getMessage());
            if (statement != null) { try { statement.close(); } catch (Exception ignored) {} }
        }
        return false;
    }

    public boolean release() {
        disconnect();
        return released.compareAndSet(false, true);
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public int hashCode() {
        return connection.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof ConnectionWrapper) && connection.equals(((ConnectionWrapper)obj).getConnection());
    }

    protected void setDriver(Driver driver) {
        this.driver = driver;
    }
}
