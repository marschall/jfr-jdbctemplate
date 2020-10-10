package com.github.marschall.jfr.jdbctemplate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import jdk.jfr.Event;
import jdk.jfr.EventType;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

final class JfrRecordingExtension implements BeforeAllCallback, AfterAllCallback {

  private final Class<? extends Event> jdbcEventClass;
  private volatile Path recordingLocation;
  private volatile Recording recording;

  JfrRecordingExtension(Class<? extends Event> jdbcEventClass) {
    this.jdbcEventClass = jdbcEventClass;
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    this.recordingLocation = this.computeRecordingPath(context);

    this.startRecording();
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    this.stopRecording();
    this.readRecording();
  }

  private Path computeRecordingPath(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    return Path.of("target", testClass.getSimpleName() + ".jfr");
  }

  private void startRecording() throws IOException {
    this.recording = new Recording();
    this.recording.enable(this.jdbcEventClass);
    this.recording.enable("org.junit.TestExecution");
    this.recording.enable("org.junit.TestPlan");
    this.recording.setMaxSize(1L * 1024L * 1024L);
    this.recording.setToDisk(true);
    this.recording.setDestination(this.recordingLocation);
    this.recording.start();
  }

  private void stopRecording() throws IOException {
    this.recording.close();
  }

  private void readRecording() throws IOException {
    Set<String> queries = new HashSet<>();
    try (RecordingFile recordingFile = new RecordingFile(this.recordingLocation)) {
      String jdbcEventClassName = this.jdbcEventClass.getName();
      while (recordingFile.hasMoreEvents()) {
        RecordedEvent event = recordingFile.readEvent();
        EventType eventType = event.getEventType();
        if (eventType.getName().equals(jdbcEventClassName)) {
          String query = event.getString("query");
          assertNotNull(query);
          queries.add(query);

          Duration duration = event.getDuration("duration");
          assertNotNull(duration);
          assertFalse(duration.isZero());
          assertFalse(duration.isNegative());
        }
      }
    }
    assertFalse(queries.isEmpty());
  }

}
