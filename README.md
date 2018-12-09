JFR JdbcTemplate [![Build Status](https://travis-ci.org/marschall/jfr-jdbctemplate.svg?branch=master)](https://travis-ci.org/marschall/jfr-jdbctemplate)
================

An implementation of Spring [JdbcTemplate](https://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#jdbc) that generates [Flight Recorder](https://openjdk.java.net/jeps/328) events.

This project requires Java 11 or later.

![Flight Recording of a JUnit Test](https://github.com/marschall/jfr-jdbctemplate/raw/master/src/main/javadoc/Screenshot%20from%202018-12-09%2000-08-53.png)

Usage
-----

You need something like the following JVM options to run Flight Recorder

```
-XX:StartFlightRecording:filename=recording.jfr
-XX:FlightRecorderOptions:stackdepth=128
```
