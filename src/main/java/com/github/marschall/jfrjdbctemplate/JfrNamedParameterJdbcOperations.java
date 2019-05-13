package com.github.marschall.jfrjdbctemplate;

import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.github.marschall.jfrjdbctemplate.JfrJdbcOperations.JdbcEvent;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
/**
 * An implementation of {@link NamedParameterJdbcOperations} that generates JFR events.
 * The events are generated in the {@value JfrConstants#CATEGORY} category.
 */
public final class JfrNamedParameterJdbcOperations implements NamedParameterJdbcOperations {

  private final NamedParameterJdbcOperations delegate;

  /**
   * Constructs a new {@link JfrNamedParameterJdbcOperations} instance.
   *
   * <p>In order to have events generated for the methods invoked on the object
   * returned by {@link #getJdbcOperations()} make sure you use the
   * {@link NamedParameterJdbcTemplate#NamedParameterJdbcTemplate(JdbcOperations)}
   * constructor with a {@link JfrJdbcOperations} object.</p>
   *
   * @param delegate the actual {@link JdbcOperations} implementation, not {@code null}
   */
  public JfrNamedParameterJdbcOperations(NamedParameterJdbcOperations delegate) {
    Objects.requireNonNull(delegate, "delegate");
    this.delegate = delegate;
  }

  @Override
  public JdbcOperations getJdbcOperations() {
    return this.delegate.getJdbcOperations();
  }

  @Override
  public <T> T execute(String sql, SqlParameterSource paramSource, PreparedStatementCallback<T> action) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("execute");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.execute(sql, paramSource, action);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(String sql, Map<String, ?> paramMap, PreparedStatementCallback<T> action) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("execute");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.execute(sql, paramMap, action);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(String sql, PreparedStatementCallback<T> action) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("execute");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.execute(sql, action);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, SqlParameterSource paramSource, ResultSetExtractor<T> rse) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.query(sql, paramSource, rse);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, Map<String, ?> paramMap, ResultSetExtractor<T> rse) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.query(sql, paramMap, rse);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, ResultSetExtractor<T> rse) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.query(sql, rse);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, SqlParameterSource paramSource, RowCallbackHandler rch) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, paramSource, rch);
      event.setRowCount(Statement.SUCCESS_NO_INFO);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, Map<String, ?> paramMap, RowCallbackHandler rch) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, paramMap, rch);
      event.setRowCount(Statement.SUCCESS_NO_INFO);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, RowCallbackHandler rch) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, rch);
      event.setRowCount(Statement.SUCCESS_NO_INFO);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.query(sql, paramSource, rowMapper);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.query(sql, paramMap, rowMapper);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, RowMapper<T> rowMapper) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.query(sql, rowMapper);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, paramSource, rowMapper);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, paramMap, rowMapper);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, SqlParameterSource paramSource, Class<T> requiredType) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, paramSource, requiredType);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Map<String, ?> paramMap, Class<T> requiredType) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, paramMap, requiredType);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql, SqlParameterSource paramSource) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      Map<String, Object> result = this.delegate.queryForMap(sql, paramSource);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql, Map<String, ?> paramMap) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      Map<String, Object> result = this.delegate.queryForMap(sql, paramMap);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, SqlParameterSource paramSource, Class<T> elementType) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.queryForList(sql, paramSource, elementType);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, Map<String, ?> paramMap, Class<T> elementType) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.queryForList(sql, paramMap, elementType);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql, SqlParameterSource paramSource) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<Map<String, Object>> result = this.delegate.queryForList(sql, paramSource);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql, Map<String, ?> paramMap) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<Map<String, Object>> result = this.delegate.queryForList(sql, paramMap);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql, SqlParameterSource paramSource) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      SqlRowSet result = this.delegate.queryForRowSet(sql, paramSource);
      event.setRowCount(Statement.SUCCESS_NO_INFO);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql, Map<String, ?> paramMap) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      SqlRowSet result = this.delegate.queryForRowSet(sql, paramMap);
      event.setRowCount(Statement.SUCCESS_NO_INFO);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, SqlParameterSource paramSource) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      int updateCount = this.delegate.update(sql, paramSource);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, Map<String, ?> paramMap) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      int updateCount = this.delegate.update(sql, paramMap);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, SqlParameterSource paramSource, KeyHolder generatedKeyHolder) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      int updateCount = this.delegate.update(sql, paramSource, generatedKeyHolder);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, SqlParameterSource paramSource, KeyHolder generatedKeyHolder, String[] keyColumnNames) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      int updateCount = this.delegate.update(sql, paramSource, generatedKeyHolder, keyColumnNames);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String sql, Map<String, ?>[] batchValues) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      int[] updateCount = this.delegate.batchUpdate(sql, batchValues);
      event.setRowCount(RowCountingUtil.countRows(updateCount));
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String sql, SqlParameterSource[] batchArgs) {
    JdbcNamedEvent event = new JdbcNamedEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      int[] updateCount = this.delegate.batchUpdate(sql, batchArgs);
      event.setRowCount(RowCountingUtil.countRows(updateCount));
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  private static void setRowCount(JdbcNamedEvent event, Object o) {
    int size = RowCountingUtil.getSize(o);
    if (size != -1) {
      event.setRowCount(size);
    } else {
      event.setRowCount(Statement.SUCCESS_NO_INFO);
    }
  }

  @Label("Named Operation")
  @Description("A named JDBC Operation")
  @Category(JfrConstants.CATEGORY)
  static class JdbcNamedEvent extends Event {

    @Label("Operation Name")
    @Description("The name of the JDBC operation")
    private String operationName;

    @Label("Query")
    @Description("The SQL query string")
    private String query;

    @Label("Row count")
    @Description("The number of rows returned or updated")
    // long instead of int to avoid overflows for batch updates
    private long rowCount;

    String getOperationName() {
      return this.operationName;
    }

    void setOperationName(String operationName) {
      this.operationName = operationName;
    }

    String getQuery() {
      return this.query;
    }

    void setQuery(String query) {
      this.query = query;
    }

    long getRowCount() {
      return rowCount;
    }

    void setRowCount(long resultSize) {
      this.rowCount = resultSize;
    }

  }

}
