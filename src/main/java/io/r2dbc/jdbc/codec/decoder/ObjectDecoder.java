/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Fallback-Decoder.
 *
 * @author Thomas Freese
 */
public class ObjectDecoder extends AbstractDecoder<Object>
{
    /**
     * Fallback-Decoder.
     */
    public static final Decoder<Object> INSTANCE = new ObjectDecoder();

    /**
     * Erstellt ein neues {@link ObjectDecoder} Object.
     */
    public ObjectDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Object doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        Object value = resultSet.getObject(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.OTHER.getVendorTypeNumber();
    }
}
