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
public interface Codecs
{
    /**
     * Returns the JavaType for the {@link JDBCType}
     *
     * @param jdbcType {@link JDBCType}
     *
     * @return Class
     */
    Class<?> getJavaType(final JDBCType jdbcType);

    /**
     * Read an Object from a {@link ResultSet}.
     *
     * @param <T> Type
     * @param jdbcType {@link JDBCType}
     * @param resultSet {@link ResultSet}
     * @param columnLabel String
     *
     * @return Object
     *
     * @throws SQLException Falls was schief geht.
     */
    <T> T mapFromSql(JDBCType jdbcType, ResultSet resultSet, String columnLabel) throws SQLException;

    /**
     * Map an Object into another one.
     *
     * @param jdbcType {@link JDBCType}
     * @param javaType Class
     * @param value Object
     *
     * @return Object
     */
    <T> T mapTo(JDBCType jdbcType, Class<? extends T> javaType, Object value);

    /**
     * Write an Object in a {@link PreparedStatement}.
     *
     * @param javaType Class
     * @param preparedStatement {@link PreparedStatement}
     * @param parameterIndex int, ONE-Based
     * @param value Object
     *
     * @throws SQLException Falls was schief geht.
     */
    void mapToSql(Class<?> javaType, PreparedStatement preparedStatement, int parameterIndex, Object value) throws SQLException;
}
