package com.github.marschall.jfrjdbctemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

class JrfJdbcOperationsTest {
  

  //-XX:+FlightRecorder
  //-XX:StartFlightRecording=duration=120s,filename=recording.jfr
  
  private SingleConnectionDataSource dataSource;
  private JdbcOperations jfrJdbcOperations;
  
  @BeforeEach
  void setUp() {
    this.dataSource = new SingleConnectionDataSource("jdbc:h2:mem:", true);
    this.jfrJdbcOperations = new JfrJdbcOperations(new JdbcTemplate(this.dataSource));
    
  }
  
  @AfterEach
  void tearDown() {
    this.dataSource.destroy();
  }

  @Test
  void simpleQuery() {
    assertEquals(Integer.valueOf(1), this.jfrJdbcOperations.queryForObject("SELECT 1 FROM dual", Integer.class));
  }
  
  @Test
  void multipleQueries() {
    this.jfrJdbcOperations.execute("CREATE TABLE t1 (c1 int, c2 int)");
    this.jfrJdbcOperations.batchUpdate("INSERT INTO t1(c1, c2) values (?, ?)", List.of(
        new Object[] {1, 2},
        new Object[] {10, 20},
        new Object[] {100, 200},
        new Object[] {1000, 2000}
        ));
    List<Integer> values = this.jfrJdbcOperations.queryForList("SELECT c1 "
        + " FROM t1 "
        + " WHERE c2 < ?"
        + " ORDER bY c1", Integer.class, 100);
    assertEquals(List.of(1, 10), values);
  }

}
