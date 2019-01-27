/**
 * Contains implementations of Spring JDBC interfaces that generate JFR
 * events.
 *
 * <table>
 * <thead>
 * <tr>
 *   <th>Spring Class</th>
 *   <th>JFR Class</th>
 * </tr>
 * </thead>
 * <tbody>
 * <tr>
 *   <td>{@link org.springframework.jdbc.core.JdbcOperations}</td>
 *   <td>{@link com.github.marschall.jfrjdbctemplate.JfrJdbcOperations}</td>
 * </tr>
 * <tr>
 *   <td>{@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations}</td>
 *   <td>{@link com.github.marschall.jfrjdbctemplate.JfrNamedParameterJdbcOperations}</td>
 * </tr>
 * </tbody>
 * </table>
 */
package com.github.marschall.jfrjdbctemplate;