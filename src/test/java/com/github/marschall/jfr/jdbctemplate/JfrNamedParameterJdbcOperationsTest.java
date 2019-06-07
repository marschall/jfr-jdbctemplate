package com.github.marschall.jfr.jdbctemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

class JfrNamedParameterJdbcOperationsTest {

  private SingleConnectionDataSource dataSource;
  private NamedParameterJdbcOperations jfrNamedJdbcOperations;

  @BeforeEach
  void setUp() {
    this.dataSource = new SingleConnectionDataSource("jdbc:h2:mem:", true);
    JfrJdbcOperations jfrdbcOperations = new JfrJdbcOperations(new JdbcTemplate(this.dataSource));
    this.jfrNamedJdbcOperations = new JfrNamedParameterJdbcOperations(new NamedParameterJdbcTemplate(jfrdbcOperations));
  }

  @AfterEach
  void tearDown() {
    this.dataSource.destroy();
  }

  @Test
  void simpleQuery() {
    assertEquals(Integer.valueOf(1), this.jfrNamedJdbcOperations.queryForObject(
            "SELECT 1 FROM dual WHERE 1 < :arg", Map.of("arg", 2),
            Integer.class));
  }

  @Test
  void multipleQueries() {
    this.jfrNamedJdbcOperations.getJdbcOperations().execute("CREATE TABLE t1 (c1 int, c2 int)");
    this.jfrNamedJdbcOperations.batchUpdate("INSERT INTO t1(c1, c2) values (:c1, :c2)", new Map[] {
        Map.of("c1", 1, "c2", 2),
        Map.of("c1", 10, "c2", 20),
        Map.of("c1", 100, "c2", 200),
        Map.of("c1", 1000, "c2", 2000)
    });
    List<Integer> values = this.jfrNamedJdbcOperations.queryForList("SELECT c1 "
        + " FROM t1 "
        + " WHERE c2 < :limit"
        + " ORDER bY c1", Map.of("limit", 100), Integer.class);
    assertEquals(List.of(1, 10), values);
  }

}
