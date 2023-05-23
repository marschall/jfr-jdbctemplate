JFR JdbcTemplate [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.marschall/jfr-jdbctemplate/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.marschall/jfr-jdbctemplate) [![Javadocs](https://www.javadoc.io/badge/com.github.marschall/jfr-jdbctemplate.svg)](https://www.javadoc.io/doc/com.github.marschall/jfr-jdbctemplate) [![Build Status](https://travis-ci.org/marschall/jfr-jdbctemplate.svg?branch=master)](https://travis-ci.org/marschall/jfr-jdbctemplate)
================

An implementation of Spring [JdbcTemplate](https://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#jdbc) that generates [Flight Recorder](https://openjdk.java.net/jeps/328) events.

This project requires Java 11 based on OpenJDK or later.

```xml
<dependency>
  <groupId>com.github.marschall</groupId>
  <artifactId>jfr-jdbctemplate</artifactId>
  <version>2.0.0</version>
</dependency>
```

Versions 1.x are intended for Spring 5.x / Java 11, versions 2.x are intended for Spring 6.x / Java 17.

![Flight Recording of a JUnit Test](https://github.com/marschall/jfr-jdbctemplate/raw/master/src/main/javadoc/resources/Screenshot%20from%202019-05-13%2021-09-33.png)

Compared to approaches based on `DataSource` an approach based on `JdbcTemplate` has the advantage that it captures a complete database interaction. For example if you process many rows the initial `PreparedStatement#execute()` might be fast but most of the time may be spent in `ResultSet#next()`. A `JdbcTemplate` based approach generates a single JFR event for the entire interaction that involves several JDBC method invocations.

 Spring Class                                                             | JFR Class                                                              |
|-------------------------------------------------------------------------|------------------------------------------------------------------------|
| `org.springframework.jdbc.core.JdbcOperations`                          | `com.github.marschall.jfr.jdbctemplate.JfrJdbcOperations`               |
| `org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations` | `com.github.marschall.jfr.jdbctemplate.JfrNamedParameterJdbcOperations` |

Reported Attributes
-------------------

<dl>
<dt>operationName</dt>
<dd>The name of the execute JDBC operation, this corresponds to the method name on <code>JdbcOperations</code>/<code>JdbcTemplate</code>.</dd>
<dt>query</dt>
<dd>The SQL query string passed to the JDBC driver. May be missing especially if custom <code>org.springframework.jdbc.core.PreparedStatementCreator</code> fail to implement <code>org.springframework.jdbc.core.SqlProvider</code>.</dd>
<dt>rowCount</dt>
<dd>In the case of a <code>SELECT</code> the number of rows returned. In the case of an <code>UPDATE</code> or <code>DELETE</code> the number of rows affected. <code>-1</code> for a statement that does not return anything like a DDL. <code>-2</code> when no information about the number of rows is available.</dd>
</dl>

Overhead
--------

We try to keep overhead to a minimum and have no additional allocations besides the JFR events. Besides the overhead of the event the only additional overhead is:

* a wrapper around `JdbcTemplate`
* a few `instanceof` operations and casts
* a `finally` block
* a capturing lambda for `#queryForStream` methods to record `Stream#close` as the end time of the event
* a small wrapper around every `RowCallbackHandler`

We assume `org.springframework.jdbc.core.SqlProvider#getSql()` is a simple getter.

Usage
-----

```java
@Configuration
public class JdbcConfiguration {

   @Autowired
   private DataSource dataSource;

   @Bean
   public JdbcOperations jdbcOperations() {
     return new JfrJdbcOperations(new JdbcTemplate(this.dataSource));
   }

   @Bean
   public NamedParameterJdbcOperations namedParameterJdbcOperations() {
     return new JfrNamedParameterJdbcOperations(new NamedParameterJdbcTemplate(this.jdbcOperations()));
   }

}
```

You need something like the following JVM options to run Flight Recorder

```
-XX:StartFlightRecording:filename=recording.jfr
-XX:FlightRecorderOptions:stackdepth=128
```

Limitations
-----------

* When the SQL query is not provided as a `String` but as a `PreparedStatementCreator` or `CallableStatementCreator` it has to implement `SqlProvider` for the query string to show up in the flight recording.
* `JdbcTemplate#query(PreparedStatementCreator, PreparedStatementSetter, ResultSetExtractor)` is not available because it is defined on `JdbcTemplate` and not `JdbcOperations`.
* Several spring-jdbc classes `AbstractJdbcCall`, `SimpleJdbcCall`, `StoredProcedure`, `RdbmsOperation`, `AbstractJdbcInsert`, `SimpleJdbcInsert` but also `JdbcTestUtils` and `JdbcBeanDefinitionReader` require a `JdbcTemplate` and do not work with `JdbcOperations`. We have a [pull request](https://github.com/spring-projects/spring-framework/pull/23066/files) open for this but it has not been merged yet.
* `JdbcOperations#execute(ConnectionCallback)` can not provide any insight into what is executed inside, that would require integration with [marschall/jfr-jdbc](https://github.com/marschall/jfr-jdbc)
