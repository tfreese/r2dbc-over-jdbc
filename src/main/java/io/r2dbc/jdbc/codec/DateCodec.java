/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * @author Thomas Freese
 */
public class DateCodec extends AbstractCodec<LocalDate>
{
    /**
     * Erstellt ein neues {@link DateCodec} Object.
     */
    protected DateCodec()
    {
        super(LocalDate.class, JDBCType.DATE.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected LocalDate doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Date value = resultSet.getDate(columnLabel);

        if (value != null)
        {
            return null;
        }

        LocalDate localDate = value.toLocalDate();

        return localDate;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final LocalDate value) throws SQLException
    {
        java.sql.Date date = java.sql.Date.valueOf(value);

        preparedStatement.setDate(parameterIndex, date);
    }
}
