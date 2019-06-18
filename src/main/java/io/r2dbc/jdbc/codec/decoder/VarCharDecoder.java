/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class VarCharDecoder extends AbstractDecoder<String>
{
    /**
     * Erstellt ein neues {@link VarCharDecoder} Object.
     */
    public VarCharDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected String doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        String value = resultSet.getString(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.VARCHAR.getVendorTypeNumber();
    }
}
