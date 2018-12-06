package com.github.marschall.jfrjdbctemplate;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JrfJdbcOperationsTest {

  @Test
  void simpleQuery() {
    SingleConnectionDataSource dataSource = new SingleConnectionDataSource("jdbc:h2:mem:", true);
    try {
      JdbcOperations jfrJdbcOperations = new JfrJdbcOperations(new JdbcTemplate(dataSource));
      assertEquals(Integer.valueOf(1), jfrJdbcOperations.queryForObject("SELECT 1 FROM dual", Integer.class));
    } finally {
      dataSource.destroy();
    }
  }

}
