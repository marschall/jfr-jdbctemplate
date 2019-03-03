package com.github.marschall.jfrjdbctemplate;

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

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
/**
 * An implementation of {@link NamedParameterJdbcOperations} that generates JFR events.
 * The events are generated in the {@value JdbcNamedEvent#CATEGORY} category.
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
    var event = new JdbcNamedEvent();
    event.setOperationName("execute");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.execute(sql, paramSource, action);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(String sql, Map<String, ?> paramMap, PreparedStatementCallback<T> action) {
    var event = new JdbcNamedEvent();
    event.setOperationName("execute");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.execute(sql, paramMap, action);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(String sql, PreparedStatementCallback<T> action) {
    var event = new JdbcNamedEvent();
    event.setOperationName("execute");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.execute(sql, action);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, SqlParameterSource paramSource, ResultSetExtractor<T> rse) {
    var event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, paramSource, rse);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, Map<String, ?> paramMap, ResultSetExtractor<T> rse) {
    var event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, paramMap, rse);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, ResultSetExtractor<T> rse) {
    var event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, rse);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, SqlParameterSource paramSource, RowCallbackHandler rch) {
    var event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, paramSource, rch);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, Map<String, ?> paramMap, RowCallbackHandler rch) {
    var event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, paramMap, rch);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, RowCallbackHandler rch) {
    var event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, rch);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper) {
    var event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, paramSource, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper) {
    var event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, paramMap, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, RowMapper<T> rowMapper) {
    var event = new JdbcNamedEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, paramSource, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, paramMap, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, SqlParameterSource paramSource, Class<T> requiredType) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, paramSource, requiredType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Map<String, ?> paramMap, Class<T> requiredType) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, paramMap, requiredType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql, SqlParameterSource paramSource) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForMap(sql, paramSource);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql, Map<String, ?> paramMap) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForMap(sql, paramMap);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, SqlParameterSource paramSource, Class<T> elementType) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, paramSource, elementType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, Map<String, ?> paramMap, Class<T> elementType) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, paramMap, elementType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql, SqlParameterSource paramSource) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, paramSource);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql, Map<String, ?> paramMap) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, paramMap);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql, SqlParameterSource paramSource) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForRowSet(sql, paramSource);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql, Map<String, ?> paramMap) {
    var event = new JdbcNamedEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForRowSet(sql, paramMap);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, SqlParameterSource paramSource) {
    var event = new JdbcNamedEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.update(sql, paramSource);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, Map<String, ?> paramMap) {
    var event = new JdbcNamedEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.update(sql, paramMap);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, SqlParameterSource paramSource, KeyHolder generatedKeyHolder) {
    var event = new JdbcNamedEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.update(sql, paramSource, generatedKeyHolder);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, SqlParameterSource paramSource, KeyHolder generatedKeyHolder, String[] keyColumnNames) {
    var event = new JdbcNamedEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.update(sql, paramSource, generatedKeyHolder, keyColumnNames);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String sql, Map<String, ?>[] batchValues) {
    var event = new JdbcNamedEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.batchUpdate(sql, batchValues);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String sql, SqlParameterSource[] batchArgs) {
    var event = new JdbcNamedEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.batchUpdate(sql, batchArgs);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Label("Named Operation")
  @Description("A named JDBC Operation")
  @Category(JdbcNamedEvent.CATEGORY)
  static class JdbcNamedEvent extends Event {

    static final String CATEGORY = "Spring JDBC";

    @Label("Operation Name")
    @Description("The name of the JDBC operation")
    private String operationName;

    @Label("Query")
    @Description("The SQL query string")
    private String query;

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

  }

}
