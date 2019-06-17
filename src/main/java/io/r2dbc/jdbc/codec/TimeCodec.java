/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;

/**
 * @author Thomas Freese
 */
public class TimeCodec extends AbstractCodec<LocalTime>
{
    /**
     * Erstellt ein neues {@link TimeCodec} Object.
     */
    protected TimeCodec()
    {
        super(LocalTime.class, JDBCType.TIME.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected LocalTime doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Time value = resultSet.getTime(columnLabel);

        if (value != null)
        {
            return null;
        }

        LocalTime localTime = value.toLocalTime();

        return localTime;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final LocalTime value) throws SQLException
    {
        java.sql.Time time = java.sql.Time.valueOf(value);

        preparedStatement.setTime(parameterIndex, time);
    }
}
