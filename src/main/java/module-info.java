module com.github.marschall.jfrjdbctemplate {

  requires java.sql;

  requires jdk.jfr;

  requires spring.beans; // required for tests
  requires spring.core;
  requires spring.tx;
  requires transitive spring.jdbc;

  exports com.github.marschall.jfr.jdbctemplate;

}