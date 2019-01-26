JFR JdbcTemplate [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.marschall/jfr-jdbctemplate/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.marschall/jfr-jdbctemplate) [![Javadocs](https://www.javadoc.io/badge/com.github.marschall/jfr-jdbctemplate.svg)](https://www.javadoc.io/doc/com.github.marschall/jfr-jdbctemplate) [![Build Status](https://travis-ci.org/marschall/jfr-jdbctemplate.svg?branch=master)](https://travis-ci.org/marschall/jfr-jdbctemplate)
================

An implementation of Spring [JdbcTemplate](https://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#jdbc) that generates [Flight Recorder](https://openjdk.java.net/jeps/328) events.

This project requires Java 11 or later.

![Flight Recording of a JUnit Test](https://github.com/marschall/jfr-jdbctemplate/raw/master/src/main/javadoc/Screenshot%20from%202018-12-09%2000-08-53.png)

Compared to approaches based on `DataSource` an approach based on `JdbcTemplate` has the advantage that it captures a complete database interaction. For example if you process many rows the initial `PreparedStatement#execute()` might be fast but most of the time may be spent in `ResultSet#next()`. A `JdbcTemplate` based approach generates a single JFR event for the entire interaction that involves several JDBC method invocations.

Usage
-----

You need something like the following JVM options to run Flight Recorder

```
-XX:StartFlightRecording:filename=recording.jfr
-XX:FlightRecorderOptions:stackdepth=128
```
