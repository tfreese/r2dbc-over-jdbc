/**
 * Created: 18.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Decodes a SQL-Type from the {@link ResultSet} to an Java-Object.
 *
 * @author Thomas Freese
 * @param <T> Type
 */
public interface SqlDecoder<T>
{
    /**
     * @param resultSet {@link ResultSet}
     * @param columnLabel String
     * @return Object
     * @throws SQLException Falls was schief geht.
     */
    public T decode(ResultSet resultSet, String columnLabel) throws SQLException;

    /**
     * @see Types
     * @see JDBCType
     * @return int
     */
    public int getSqlType();
}
