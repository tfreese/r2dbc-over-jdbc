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
public class IntegerDecoder extends AbstractDecoder<Integer>
{
    /**
     * Erstellt ein neues {@link IntegerDecoder} Object.
     */
    public IntegerDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Integer doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        int value = resultSet.getInt(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.INTEGER.getVendorTypeNumber();
    }
}
