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
public class ObjectDecoder extends AbstractSqlDecoder<Object>
{
    /**
     * Fallback-Decoder.
     */
    public static final SqlDecoder<Object> INSTANCE = new ObjectDecoder();

    /**
     * Erstellt ein neues {@link ObjectDecoder} Object.
     */
    public ObjectDecoder()
    {
        super(JDBCType.OTHER.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Object decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        Object value = resultSet.getObject(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }
}
