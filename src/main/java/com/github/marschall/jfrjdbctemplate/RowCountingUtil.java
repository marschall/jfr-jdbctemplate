package com.github.marschall.jfrjdbctemplate;

import java.lang.reflect.Array;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;

final class RowCountingUtil {

  private RowCountingUtil() {
    throw new AssertionError("not instantiable");
  }

  static int getSize(Object o) {
    if (o == null) {
      return -1;
    }
    if (o instanceof Collection) {
      return ((Collection<?>) o).size();
    }
    if (o instanceof Map) {
      return ((Map<?, ?>) o).size();
    }
    if (o.getClass().isArray()) {
      return Array.getLength(o);
    }
    return -1;
  }

  static long countRows(int[] updateCount) {
    long count = 0L;
    for (int i : updateCount) {
      if (i == Statement.SUCCESS_NO_INFO) {
        return Statement.SUCCESS_NO_INFO;
      }
      count += i;
    }
    return count;
  }

  static long countRows(int[][] updateCounts) {
    long count = 0L;
    for (int[] updateCount : updateCounts) {
      long i = RowCountingUtil.countRows(updateCount);
      if (i == Statement.SUCCESS_NO_INFO) {
        return Statement.SUCCESS_NO_INFO;
      }
      count += i;
    }
    return count;
  }

}
