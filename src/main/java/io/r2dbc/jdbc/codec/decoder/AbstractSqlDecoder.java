/**
 * Created: 18.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.ResultSet;

/**
 * Decodes a SQL-Type from the {@link ResultSet} to an Java-Object.
 *
 * @author Thomas Freese
 * @param <T> Type
 */
public abstract class AbstractSqlDecoder<T> implements SqlDecoder<T>
{
    /**
     *
     */
    private final int sqlType;

    /**
     * Erstellt ein neues {@link AbstractSqlDecoder} Object.
     * 
     * @param sqlType int
     */
    public AbstractSqlDecoder(final int sqlType)
    {
        super();

        this.sqlType = sqlType;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return this.sqlType;
    }
}
