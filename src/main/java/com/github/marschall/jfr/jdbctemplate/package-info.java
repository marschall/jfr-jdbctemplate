/**
 * Contains implementations of Spring JDBC interfaces that generate JFR
 * events.
 *
 * <table>
 * <caption>JFR Support Classes</caption>
 * <thead>
 * <tr>
 *   <th>Spring Class</th>
 *   <th>JFR Class</th>
 * </tr>
 * </thead>
 * <tbody>
 * <tr>
 *   <td>{@link org.springframework.jdbc.core.JdbcOperations}</td>
 *   <td>{@link com.github.marschall.jfr.jdbctemplate.JfrJdbcOperations}</td>
 * </tr>
 * <tr>
 *   <td>{@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations}</td>
 *   <td>{@link com.github.marschall.jfr.jdbctemplate.JfrNamedParameterJdbcOperations}</td>
 * </tr>
 * </tbody>
 * </table>
 *
 * <h2>Screenshot</h2>
 * <img src="{@docRoot}/resources/Screenshot%20from%202019-05-13%2021-09-33.png" alt="Java Flight Recoder Screenshot">
 */
package com.github.marschall.jfr.jdbctemplate;