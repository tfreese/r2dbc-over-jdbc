/**
 * Created: 18.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Decodes a SQL-Type from the {@link ResultSet} to an Java-Object.
 *
 * @author Thomas Freese
 * @param <T> Type
 */
public abstract class AbstractDecoder<T> implements Decoder<T>
{
    /**
     * Erstellt ein neues {@link AbstractDecoder} Object.
     */
    public AbstractDecoder()
    {
        super();
    }

    /**
     * @param resultSet {@link ResultSet}
     * @param value Object
     * @return Object
     * @throws SQLException Falls was schief geht.
     */
    protected T checkWasNull(final ResultSet resultSet, final T value) throws SQLException
    {
        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public final T decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        T value = doDecode(resultSet, columnLabel);

        value = checkWasNull(resultSet, value);

        return value;
    }

    /**
     * @param resultSet {@link ResultSet}
     * @param columnLabel String
     * @return Object
     * @throws SQLException Falls was schief geht.
     */
    protected abstract T doDecode(ResultSet resultSet, String columnLabel) throws SQLException;
}
