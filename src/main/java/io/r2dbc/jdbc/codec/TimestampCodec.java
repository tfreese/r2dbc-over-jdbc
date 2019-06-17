/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

/**
 * @author Thomas Freese
 */
public class TimestampCodec extends AbstractCodec<LocalDateTime>
{
    /**
     * Erstellt ein neues {@link TimestampCodec} Object.
     */
    protected TimestampCodec()
    {
        super(LocalDateTime.class, JDBCType.TIMESTAMP.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected LocalDateTime doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Timestamp value = resultSet.getTimestamp(columnLabel);

        if (value != null)
        {
            return null;
        }

        LocalDateTime localDateTime = value.toLocalDateTime();

        return localDateTime;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final LocalDateTime value) throws SQLException
    {
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(value);

        preparedStatement.setTimestamp(parameterIndex, timestamp);
    }
}
