// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Codex for a SQL-Type from the {@link ResultSet} to an Java-Object and vice versa.
 *
 * @author Thomas Freese
 * @param <T> Type
 */
public interface Codec<T>
{
    /**
     * Can read this {@link JDBCType} from a {@link ResultSet} ?
     *
     * @param jdbcType {@link JDBCType}
     * @return boolean
     */
    public boolean canMapFromSql(JDBCType jdbcType);

    /**
     * Can map this {@link JDBCType} to another Object-Type ?
     *
     * @param jdbcType {@link JDBCType}
     * @param type Class
     * @return boolean
     */
    public boolean canMapTo(JDBCType jdbcType, Class<?> type);

    /**
     * Can write this {@link JDBCType} to a {@link PreparedStatement} ?
     *
     * @param jdbcType {@link JDBCType}
     * @param value Object
     * @return boolean
     */
    public boolean canMapToSql(JDBCType jdbcType, Object value);

    /**
     * Map an Object into another one.
     *
     * @param value Object
     * @return Object
     */
    public T map(Object value);

    /**
     * Read an Object from a {@link ResultSet}.
     *
     * @param resultSet {@link ResultSet}
     * @param columnLabel String
     * @return Object
     * @throws SQLException Falls was schief geht.
     */
    public T mapFromSql(ResultSet resultSet, String columnLabel) throws SQLException;

    /**
     * Write an Object in a {@link PreparedStatement}.
     *
     * @param preparedStatement {@link PreparedStatement}
     * @param parameterIndex int, ONE-Based
     * @param value Object
     * @throws SQLException Falls was schief geht.
     */
    public void mapToSql(PreparedStatement preparedStatement, int parameterIndex, T value) throws SQLException;
}
