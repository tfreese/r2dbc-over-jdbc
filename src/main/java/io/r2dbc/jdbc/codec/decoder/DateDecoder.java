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
public class DateDecoder extends AbstractSqlDecoder<Date>
{
    /**
     * Erstellt ein neues {@link DateDecoder} Object.
     */
    public DateDecoder()
    {
        super(JDBCType.DATE.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Date decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Date value = resultSet.getDate(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        // java.sql.Date -> java.util.Date
        return new Date(value.getTime());
    }
}
