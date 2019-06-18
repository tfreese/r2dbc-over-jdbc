/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * @author Thomas Freese
 */
public class DateDecoder extends AbstractDecoder<Date>
{
    /**
     * Erstellt ein neues {@link DateDecoder} Object.
     */
    public DateDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.DATE.getVendorTypeNumber();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#checkWasNull(java.sql.ResultSet, java.lang.Object)
     */
    @Override
    protected Date checkWasNull(final ResultSet resultSet, final Date value) throws SQLException
    {
        if (resultSet.wasNull())
        {
            return null;
        }

        // java.sql.Date -> java.util.Date
        return new Date(value.getTime());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Date doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Date value = resultSet.getDate(columnLabel);

        return value;
    }
}
