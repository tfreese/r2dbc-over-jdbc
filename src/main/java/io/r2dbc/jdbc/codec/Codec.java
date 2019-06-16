/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Encodes and decodes an object.
 *
 * @author Thomas Freese
 * @param <T> Type
 */
public interface Codec<T>
{
    /**
     * @param resultSet {@link ResultSet}
     * @param columnLabel String
     * @return Object
     * @throws SQLException Falls was schief geht.
     */
    public T decode(ResultSet resultSet, String columnLabel) throws SQLException;

    /**
     * @param preparedStatement {@link PreparedStatement}
     * @param parameterIndex int, ONE-Based
     * @param value Object
     * @throws SQLException Falls was schief geht.
     */
    public void encode(PreparedStatement preparedStatement, int parameterIndex, T value) throws SQLException;

    /**
     * @return {@link Class}
     */
    public Class<T> getJavaType();

    /**
     * @see Types
     * @see JDBCType
     * @return int
     */
    public int getSqlType();
}
