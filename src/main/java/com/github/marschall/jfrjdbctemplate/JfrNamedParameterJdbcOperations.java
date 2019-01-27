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

import com.github.marschall.jfrjdbctemplate.JfrJdbcOperations.JdbcEvent;
/**
 * An implementation of {@link NamedParameterJdbcOperations} that generates JFR events.
 * The events are generated in the "JDBC" category.
 */
public final class JfrNamedParameterJdbcOperations implements NamedParameterJdbcOperations {

  private final NamedParameterJdbcOperations delegate;

  /**
   * Constructs a new {@link JfrNamedParameterJdbcOperations} instance
   *
   * <p>In order to have events generated for the methods invoked on the object
   * returned by {@link #getJdbcOperations()} make sure you use the
   * {@link NamedParameterJdbcTemplate#NamedParameterJdbcTemplate(JdbcOperations)}
   * constructor with a {@link JfrJdbcOperations} object.</p>
   *
   * @param delegate the actual {@link JdbcOperations} implementation
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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
    JdbcEvent event = new JdbcEvent();
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

}
