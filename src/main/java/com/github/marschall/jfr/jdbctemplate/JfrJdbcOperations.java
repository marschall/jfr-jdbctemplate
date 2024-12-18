package com.github.marschall.jfr.jdbctemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.SqlProvider;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;

/**
 * An implementation of {@link JdbcOperations} that generates JFR events.
 * The events are generated in the {@value JfrConstants#CATEGORY} category.
 */
public final class JfrJdbcOperations implements JdbcOperations {

  // http://hirt.se/blog/?p=870
  // https://www.oracle.com/technetwork/oem/soa-mgmt/con10912-javaflightrecorder-2342054.pdf

  private static final int NO_ROWS = -1;
  private final JdbcOperations delegate;

  /**
   * Constructs a new {@link JfrJdbcOperations}.
   *
   * @param delegate the actual {@link JdbcOperations} implementation, not {@code null}
   */
  public JfrJdbcOperations(JdbcOperations delegate) {
    Objects.requireNonNull(delegate, "delegate");
    this.delegate = delegate;
  }

  @Override
  public <T> T execute(ConnectionCallback<T> action) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(getSql(action));
    event.begin();
    try {
      T result = this.delegate.execute(action);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(StatementCallback<T> action) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("execute");
    event.begin();
    try {
      T result = this.delegate.execute(action);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void execute(String sql) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.execute(sql);
      event.setRowCount(NO_ROWS);
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
      T result = this.delegate.query(sql, rse);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, RowCallbackHandler rch) {
    CountingRowCallbackHandler countingRowCallbackHandler = new CountingRowCallbackHandler(rch);
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, countingRowCallbackHandler);
      event.setRowCount(countingRowCallbackHandler.getRowCount());
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
      List<T> result = this.delegate.query(sql, rowMapper);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, RowMapper<T> rowMapper) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, rowMapper);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Class<T> requiredType) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, requiredType);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      Map<String, Object> result = this.delegate.queryForMap(sql);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, Class<T> elementType) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.queryForList(sql, elementType);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<Map<String, Object>> result = this.delegate.queryForList(sql);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      SqlRowSet result = this.delegate.queryForRowSet(sql);
      event.setRowCount(Statement.SUCCESS_NO_INFO);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      int updateCount = this.delegate.update(sql);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String... sql) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    if ((sql != null) && (sql.length > 0)) {
      event.setQuery(sql[0]);
    }
    event.begin();
    try {
      int[] updateCount = this.delegate.batchUpdate(sql);
      event.setRowCount(RowCountingUtil.countRows(updateCount));
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      T result = this.delegate.execute(psc, action);
      setRowCount(event, result);
      return result;
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
      T result = this.delegate.execute(sql, action);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      T result = this.delegate.query(psc, rse);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, PreparedStatementSetter pss, ResultSetExtractor<T> rse) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.query(sql, pss, rse);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, Object[] args, int[] argTypes, ResultSetExtractor<T> rse) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.query(sql, args, argTypes, rse);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  @Deprecated
  public <T> T query(String sql, Object[] args, ResultSetExtractor<T> rse) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.query(sql, args, rse);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, ResultSetExtractor<T> rse, Object... args) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.query(sql, rse, args);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(PreparedStatementCreator psc, RowCallbackHandler rch) {
    CountingRowCallbackHandler countingRowCallbackHandler = new CountingRowCallbackHandler(rch);
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      this.delegate.query(psc, countingRowCallbackHandler);
      event.setRowCount(countingRowCallbackHandler.getRowCount());
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, PreparedStatementSetter pss, RowCallbackHandler rch) {
    CountingRowCallbackHandler countingRowCallbackHandler = new CountingRowCallbackHandler(rch);
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, pss, countingRowCallbackHandler);
      event.setRowCount(countingRowCallbackHandler.getRowCount());
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, Object[] args, int[] argTypes, RowCallbackHandler rch) {
    CountingRowCallbackHandler countingRowCallbackHandler = new CountingRowCallbackHandler(rch);
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, args, argTypes, countingRowCallbackHandler);
      event.setRowCount(countingRowCallbackHandler.getRowCount());
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  @Deprecated
  public void query(String sql, Object[] args, RowCallbackHandler rch) {
    CountingRowCallbackHandler countingRowCallbackHandler = new CountingRowCallbackHandler(rch);
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, args, countingRowCallbackHandler);
      event.setRowCount(countingRowCallbackHandler.getRowCount());
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, RowCallbackHandler rch, Object... args) {
    CountingRowCallbackHandler countingRowCallbackHandler = new CountingRowCallbackHandler(rch);
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, countingRowCallbackHandler, args);
      event.setRowCount(countingRowCallbackHandler.getRowCount());
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      List<T> result = this.delegate.query(psc, rowMapper);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, PreparedStatementSetter pss, RowMapper<T> rowMapper) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.query(sql, pss, rowMapper);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, Object[] args, int[] argTypes, RowMapper<T> rowMapper) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.query(sql, args, argTypes, rowMapper);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  @Deprecated
  public <T> List<T> query(String sql, Object[] args, RowMapper<T> rowMapper) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.query(sql, args, rowMapper);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.query(sql, rowMapper, args);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Object[] args, int[] argTypes, RowMapper<T> rowMapper) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, args, argTypes, rowMapper);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  @Deprecated
  public <T> T queryForObject(String sql, Object[] args, RowMapper<T> rowMapper) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, args, rowMapper);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, RowMapper<T> rowMapper, Object... args) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, rowMapper, args);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Object[] args, int[] argTypes, Class<T> requiredType) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, args, argTypes, requiredType);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  @Deprecated
  public <T> T queryForObject(String sql, Object[] args, Class<T> requiredType) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, args, requiredType);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      T result = this.delegate.queryForObject(sql, requiredType, args);
      event.setRowCount(1L);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql, Object[] args, int[] argTypes) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      Map<String, Object> result = this.delegate.queryForMap(sql, args, argTypes);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql, Object... args) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      Map<String, Object> result = this.delegate.queryForMap(sql, args);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, Object[] args, int[] argTypes, Class<T> elementType) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.queryForList(sql, args, argTypes, elementType);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  @Deprecated
  public <T> List<T> queryForList(String sql, Object[] args, Class<T> elementType) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.queryForList(sql, args, elementType);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, Class<T> elementType, Object... args) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<T> result = this.delegate.queryForList(sql, elementType, args);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql, Object[] args, int[] argTypes) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<Map<String, Object>> result = this.delegate.queryForList(sql, args, argTypes);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql, Object... args) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      List<Map<String, Object>> result = this.delegate.queryForList(sql, args);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql, Object[] args, int[] argTypes) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      SqlRowSet result = this.delegate.queryForRowSet(sql, args, argTypes);
      event.setRowCount(Statement.SUCCESS_NO_INFO);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql, Object... args) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      SqlRowSet result = this.delegate.queryForRowSet(sql, args);
      event.setRowCount(Statement.SUCCESS_NO_INFO);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> Stream<T> queryForStream(String sql, RowMapper<T> rowMapper) throws DataAccessException {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForStream");
    event.setQuery(sql);
    event.begin();
    return this.delegate.queryForStream(sql, rowMapper)
            .onClose(() -> {
              event.end();
              event.commit();
            });
  }

  @Override
  public <T> Stream<T> queryForStream(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForStream");
    event.setQuery(getSql(psc));
    event.begin();
    return this.delegate.queryForStream(psc, rowMapper)
            .onClose(() -> {
              event.end();
              event.commit();
            });
  }

  @Override
  public <T> Stream<T> queryForStream(String sql, PreparedStatementSetter pss, RowMapper<T> rowMapper) throws DataAccessException {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForStream");
    event.setQuery(sql);
    event.begin();
    return this.delegate.queryForStream(sql, pss, rowMapper)
            .onClose(() -> {
              event.end();
              event.commit();
            });
  }

  @Override
  public <T> Stream<T> queryForStream(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("queryForStream");
    event.setQuery(sql);
    event.begin();
    return this.delegate.queryForStream(sql, rowMapper, args)
            .onClose(() -> {
              event.end();
              event.commit();
            });
  }

  @Override
  public int update(PreparedStatementCreator psc) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      int updateCount = this.delegate.update(psc);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(PreparedStatementCreator psc, KeyHolder generatedKeyHolder) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(getSql(generatedKeyHolder));
    event.begin();
    try {
      int updateCount = this.delegate.update(psc, generatedKeyHolder);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, PreparedStatementSetter pss) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      int updateCount = this.delegate.update(sql, pss);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, Object[] args, int[] argTypes) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      int updateCount = this.delegate.update(sql, args, argTypes);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, Object... args) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      int updateCount = this.delegate.update(sql, args);
      event.setRowCount(updateCount);
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String sql, BatchPreparedStatementSetter pss) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      int[] updateCount = this.delegate.batchUpdate(sql, pss);
      event.setRowCount(RowCountingUtil.countRows(updateCount));
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String sql, List<Object[]> batchArgs) {
    JdbcEvent event = new JdbcEvent();
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

  @Override
  public int[] batchUpdate(String sql, List<Object[]> batchArgs, int[] argTypes) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      int[] updateCount = this.delegate.batchUpdate(sql, batchArgs, argTypes);
      event.setRowCount(RowCountingUtil.countRows(updateCount));
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> int[][] batchUpdate(String sql, Collection<T> batchArgs, int batchSize, ParameterizedPreparedStatementSetter<T> pss) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      int[][] updateCount = this.delegate.batchUpdate(sql, batchArgs, batchSize, pss);
      event.setRowCount(RowCountingUtil.countRows(updateCount));
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }
  
  @Override
  public int[] batchUpdate(PreparedStatementCreator psc, BatchPreparedStatementSetter pss, KeyHolder generatedKeyHolder) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      int[] updateCount = this.delegate.batchUpdate(psc, pss, generatedKeyHolder);
      event.setRowCount(RowCountingUtil.countRows(updateCount));
      return updateCount;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(CallableStatementCreator csc, CallableStatementCallback<T> action) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(getSql(csc));
    event.begin();
    try {
      T result = this.delegate.execute(csc, action);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(String callString, CallableStatementCallback<T> action) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(callString);
    event.begin();
    try {
      T result = this.delegate.execute(callString, action);
      setRowCount(event, result);
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> call(CallableStatementCreator csc, List<SqlParameter> declaredParameters) {
    JdbcEvent event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(getSql(csc));
    event.begin();
    try {
      Map<String, Object> result = this.delegate.call(csc, declaredParameters);
      event.setRowCount(result.size());
      return result;
    } finally {
      event.end();
      event.commit();
    }
  }

  private static String getSql(Object o) {
    if (o instanceof SqlProvider) {
      return ((SqlProvider) o).getSql();
    }
    return null;
  }

  private static void setRowCount(JdbcEvent event, Object o) {
    int size = RowCountingUtil.getSize(o);
    if (size != -1) {
      event.setRowCount(size);
    } else {
      event.setRowCount(Statement.SUCCESS_NO_INFO);
    }
  }

  @Label("Operation")
  @Description("A JDBC Operation")
  @Category(JfrConstants.CATEGORY)
  static class JdbcEvent extends Event {

    @Label("Operation Name")
    @Description("The name of the JDBC operation")
    private String operationName;

    @Label("Query")
    @Description("The SQL query string")
    private String query;

    @Label("Row Count")
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
      return this.rowCount;
    }

    void setRowCount(long resultSize) {
      this.rowCount = resultSize;
    }

  }

  static final class CountingRowCallbackHandler implements RowCallbackHandler {

    private long count;

    private final RowCallbackHandler delegate;

    CountingRowCallbackHandler(RowCallbackHandler delegate) {
      this.delegate = delegate;
      this.count = 0L;
    }

    @Override
    public void processRow(ResultSet rs) throws SQLException {
      this.count += 1L;
      this.delegate.processRow(rs);
    }

    long getRowCount() {
      return this.count;
    }

  }

}
