package com.github.marschall.jfr.jdbctemplate;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

public class JfrJdbcOperationsReader {

  public static void main(String[] args) throws IOException {
    Path path = Paths.get("/home/marschall/git/jfr-demo/recording.jfr");
    Map<String, Duration> histogram = new HashMap<>();
    try (RecordingFile recording = new RecordingFile(path)) {
      while (recording.hasMoreEvents()) {
        RecordedEvent event = recording.readEvent();
        if (event.getEventType().getName().equals("com.github.marschall.jfrjdbctemplate.JfrJdbcOperations$JdbcEvent")) {
          String query = event.getString("query");
          Duration duration = event.getDuration();
          histogram.merge(query, duration, Duration::plus);
        }
      }
    }
    for (Entry<String, Duration> entry : histogram.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
  }

}
