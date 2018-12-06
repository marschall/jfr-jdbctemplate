JFR JdbcTemplate
================

An implementation of Spring [JdbcTemplate](https://docs.spring.io/spring/docs/current/spring-framework-reference/data-access.html#jdbc) that generates [Flight Recorder](https://openjdk.java.net/jeps/328) events.


```
-XX:+FlightRecorder
-XX:StartFlightRecording=disk=true,dumponexit=true,filename=recording.jfr
```
