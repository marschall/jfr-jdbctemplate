module com.github.marschall.jfrjdbctemplate {

  requires jdk.jfr;

  requires spring.beans; // required for tests
  requires spring.core;
  requires spring.tx;
  requires spring.jdbc;

  exports com.github.marschall.jfrjdbctemplate;

}