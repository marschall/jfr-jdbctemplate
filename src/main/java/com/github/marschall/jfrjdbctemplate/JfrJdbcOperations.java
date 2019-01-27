package com.github.marschall.jfrjdbctemplate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
 * The events are generated in the "JDBC" category.
 */
public final class JfrJdbcOperations implements JdbcOperations {

  // http://hirt.se/blog/?p=870
  // https://www.oracle.com/technetwork/oem/soa-mgmt/con10912-javaflightrecorder-2342054.pdf

  private final JdbcOperations delegate;

  /**
   * Constructs a new {@link JfrJdbcOperations}.
   * 
   * @param delegate the actual {@link JdbcOperations} implementation
   */
  public JfrJdbcOperations(JdbcOperations delegate) {
    Objects.requireNonNull(delegate, "delegate");
    this.delegate = delegate;
  }

  @Override
  public <T> T execute(ConnectionCallback<T> action) {
    var event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(getSql(action));
    event.begin();
    try {
      return this.delegate.execute(action);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(StatementCallback<T> action) {
    var event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(getSql(action));
    event.begin();
    try {
      return this.delegate.execute(action);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void execute(String sql) {
    var event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.execute(sql);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, ResultSetExtractor<T> rse) {
    var event = new JdbcEvent();
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
  public void query(String sql, RowCallbackHandler rch) {
    var event = new JdbcEvent();
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
  public <T> List<T> query(String sql, RowMapper<T> rowMapper) {
    var event = new JdbcEvent();
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
  public <T> T queryForObject(String sql, RowMapper<T> rowMapper) {
    var event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Class<T> requiredType) {
    var event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, requiredType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql) {
    var event = new JdbcEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForMap(sql);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, Class<T> elementType) {
    var event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, elementType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql) {
    var event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql) {
    var event = new JdbcEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForRowSet(sql);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql) {
    var event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.update(sql);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String... sql) {
    var event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    if (sql != null && sql.length > 0) {
      event.setQuery(sql[0]);
    }
    event.begin();
    try {
      return this.delegate.batchUpdate(sql);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) {
    var event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      return this.delegate.execute(psc, action);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(String sql, PreparedStatementCallback<T> action) {
    var event = new JdbcEvent();
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
  public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      return this.delegate.query(psc, rse);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, PreparedStatementSetter pss, ResultSetExtractor<T> rse) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, pss, rse);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, Object[] args, int[] argTypes, ResultSetExtractor<T> rse) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, args, argTypes, rse);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, Object[] args, ResultSetExtractor<T> rse) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, args, rse);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T query(String sql, ResultSetExtractor<T> rse, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, rse, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(PreparedStatementCreator psc, RowCallbackHandler rch) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      this.delegate.query(psc, rch);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, PreparedStatementSetter pss, RowCallbackHandler rch) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, pss, rch);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, Object[] args, int[] argTypes, RowCallbackHandler rch) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, args, argTypes, rch);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public void query(String sql, Object[] args, RowCallbackHandler rch) {
    var jdbcEvent = new JdbcEvent();
    jdbcEvent.setOperationName("query");
    jdbcEvent.setQuery(sql);
    jdbcEvent.begin();
    try {
      this.delegate.query(sql, args, rch);
    } finally {
      jdbcEvent.end();
      jdbcEvent.commit();
    }
  }

  @Override
  public void query(String sql, RowCallbackHandler rch, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      this.delegate.query(sql, rch, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      return this.delegate.query(psc, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, PreparedStatementSetter pss, RowMapper<T> rowMapper) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, pss, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, Object[] args, int[] argTypes, RowMapper<T> rowMapper) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, args, argTypes, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, Object[] args, RowMapper<T> rowMapper) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, args, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("query");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.query(sql, rowMapper, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Object[] args, int[] argTypes, RowMapper<T> rowMapper) {
    var event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, args, argTypes, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Object[] args, RowMapper<T> rowMapper) {
    var event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, args, rowMapper);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, RowMapper<T> rowMapper, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, rowMapper, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Object[] args, int[] argTypes, Class<T> requiredType) {
    var event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, args, argTypes, requiredType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Object[] args, Class<T> requiredType) {
    var event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, args, requiredType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("queryForObject");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForObject(sql, requiredType, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql, Object[] args, int[] argTypes) {
    var event = new JdbcEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForMap(sql, args, argTypes);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> queryForMap(String sql, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("queryForMap");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForMap(sql, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, Object[] args, int[] argTypes, Class<T> elementType) {
    var event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, args, argTypes, elementType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, Object[] args, Class<T> elementType) {
    var event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, args, elementType);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> List<T> queryForList(String sql, Class<T> elementType, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, elementType, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql, Object[] args, int[] argTypes) {
    var event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, args, argTypes);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public List<Map<String, Object>> queryForList(String sql, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("queryForList");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForList(sql, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql, Object[] args, int[] argTypes) {
    var event = new JdbcEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForRowSet(sql, args, argTypes);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public SqlRowSet queryForRowSet(String sql, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("queryForRowSet");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.queryForRowSet(sql, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(PreparedStatementCreator psc) {
    var event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(getSql(psc));
    event.begin();
    try {
      return this.delegate.update(psc);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(PreparedStatementCreator psc, KeyHolder generatedKeyHolder) {
    var event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(getSql(generatedKeyHolder));
    event.begin();
    try {
      return this.delegate.update(psc, generatedKeyHolder);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, PreparedStatementSetter pss) {
    var event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.update(sql, pss);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, Object[] args, int[] argTypes) {
    var event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.update(sql, args, argTypes);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int update(String sql, Object... args) {
    var event = new JdbcEvent();
    event.setOperationName("update");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.update(sql, args);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String sql, BatchPreparedStatementSetter pss) {
    var event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.batchUpdate(sql, pss);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public int[] batchUpdate(String sql, List<Object[]> batchArgs) {
    var event = new JdbcEvent();
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

  @Override
  public int[] batchUpdate(String sql, List<Object[]> batchArgs, int[] argTypes) {
    var event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.batchUpdate(sql, batchArgs, argTypes);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> int[][] batchUpdate(String sql, Collection<T> batchArgs, int batchSize, ParameterizedPreparedStatementSetter<T> pss) {
    var event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(sql);
    event.begin();
    try {
      return this.delegate.batchUpdate(sql, batchArgs, batchSize, pss);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(CallableStatementCreator csc, CallableStatementCallback<T> action) {
    var event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(getSql(csc));
    event.begin();
    try {
      return this.delegate.execute(csc, action);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public <T> T execute(String callString, CallableStatementCallback<T> action) {
    var event = new JdbcEvent();
    event.setOperationName("execute");
    event.setQuery(callString);
    event.begin();
    try {
      return this.delegate.execute(callString, action);
    } finally {
      event.end();
      event.commit();
    }
  }

  @Override
  public Map<String, Object> call(CallableStatementCreator csc, List<SqlParameter> declaredParameters) {
    var event = new JdbcEvent();
    event.setOperationName("batchUpdate");
    event.setQuery(getSql(csc));
    event.begin();
    try {
      return this.delegate.call(csc, declaredParameters);
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

  @Label("JDBC Operation")
  @Description("A JDBC Operation")
  @Category("JDBC")
  static class JdbcEvent extends Event {

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
