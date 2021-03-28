// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * Codex for a SQL-Type from the {@link ResultSet} to an Java-Object and vice versa.
 *
 * @author Thomas Freese
 * @param <T> Type
 */
public interface Codec<T>
{
    /**
     * @return Class
     */
    public Class<T> getJavaType();

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
     * Map an Object into another one.
     *
     * @param javaType Class
     * @param value Object
     * @return Object
     */
    public <M> M mapTo(Class<M> javaType, T value);

    /**
     * Write an Object in a {@link PreparedStatement}.
     *
     * @param preparedStatement {@link PreparedStatement}
     * @param parameterIndex int, ONE-Based
     * @param value Object
     * @throws SQLException Falls was schief geht.
     */
    public void mapToSql(PreparedStatement preparedStatement, int parameterIndex, T value) throws SQLException;

    /**
     * @return {@link Set}
     */
    public Set<JDBCType> supportedJdbcTypes();
}
