# OPOWER Connection Pool Homework

This is a very basic scaffold project for you to work in for the connection pool homework assignment

## Instructions

Please deliver your solution via pull request or an archive format of your choice, including all project files, within 1 calendar week.

Write a connection pool class that implements this interface (it is also located in `src/main/java/com/opower/connectionpool/ConnectionPool.java`):

    public interface ConnectionPool {
        java.sql.Connection getConnection() throws java.sql.SQLException;
        void releaseConnection(java.sql.Connection con) throws java.sql.SQLException;
    }

While we know there are many production-ready implementations of connection pools, this assignment allows for a variety of solutions to a real-world problem.  Your solution will be reviewed by the engineers you would be working with if you joined OPOWER.  We are interested in seeing your real-world design, coding, and testing skills.

## Using this scaffold

This scaffold is provided to help you (and us) build your homework code. 
We've included a `pom.xml`, which is a file used by [maven][maven] to build the project and run other commands.   It also contains
information on downloading dependent jars needed by your project.  This one contains JUnit, EasyMock and Log4J already, but feel free
to change it as you see fit.

    mvn compile      # compiles your code in src/main/java
    mvn test-compile # compile test code in src/test/java
    mvn test         # run tests in src/test/java for files named Test*.java


[maven]:http://maven.apache.org/

