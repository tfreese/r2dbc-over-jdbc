/**
 * Created: 18.06.2019
 */

package io.r2dbc.jdbc.codec.encoder;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Encodes a Java-Object for a {@link PreparedStatement}.
 *
 * @author Thomas Freese
 * @param <T> Type
 */
public interface Encoder<T>
{
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
