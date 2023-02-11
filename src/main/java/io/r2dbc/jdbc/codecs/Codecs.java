// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Encodes and decodes objects.
 *
 * @author Thomas Freese
 */
public interface Codecs {
    Class<?> getJavaType(JDBCType jdbcType);

    JDBCType getJdbcType(Class<?> javaType);

    <T> T mapFromSql(JDBCType jdbcType, ResultSet resultSet, String columnLabel) throws SQLException;

    <T> T mapTo(JDBCType jdbcType, Class<? extends T> javaType, Object value);

    void mapToSql(Class<?> javaType, PreparedStatement preparedStatement, int parameterIndex, Object value) throws SQLException;
}
