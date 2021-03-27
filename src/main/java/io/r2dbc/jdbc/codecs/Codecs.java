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
     * Read an Object from a {@link ResultSet}.
     *
     * @param <T> Type
     * @param jdbcType {@link JDBCType}
     * @param resultSet {@link ResultSet}
     * @param columnLabel String
     * @return Object
     * @throws SQLException Falls was schief geht.
     */
    public <T> T mapFromSql(JDBCType jdbcType, ResultSet resultSet, String columnLabel) throws SQLException;

    /**
     * Map an Object into another one.
     *
     * @param jdbcType {@link JDBCType}
     * @param type Class
     * @param value Object
     * @return Object
     */
    public <T> T mapTo(JDBCType jdbcType, Class<? extends T> type, Object value);

    /**
     * Write an Object in a {@link PreparedStatement}.
     *
     * @param <T> Type
     * @param jdbcType {@link JDBCType}
     * @param preparedStatement {@link PreparedStatement}
     * @param parameterIndex int, ONE-Based
     * @param value Object
     * @throws SQLException Falls was schief geht.
     */
    public <T> void mapToSql(JDBCType jdbcType, PreparedStatement preparedStatement, int parameterIndex, T value) throws SQLException;
}
