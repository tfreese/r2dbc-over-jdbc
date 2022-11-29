// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * Codex for a SQL-Type from the {@link ResultSet} to a Java-Object and vice versa.
 *
 * @param <T> Type
 *
 * @author Thomas Freese
 */
public interface Codec<T>
{
    Class<T> getJavaType();

    T mapFromSql(ResultSet resultSet, String columnLabel) throws SQLException;

    <M> M mapTo(Class<M> javaType, T value);

    void mapToSql(PreparedStatement preparedStatement, int parameterIndex, T value) throws SQLException;

    Set<JDBCType> supportedJdbcTypes();
}
