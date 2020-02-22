/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.encoder;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

/**
 * @author Thomas Freese
 */
public class DateEncoder extends AbstractSqlEncoder<Date>
{
    /**
     * Erstellt ein neues {@link DateEncoder} Object.
     */
    public DateEncoder()
    {
        super(JDBCType.DATE.getVendorTypeNumber());
    }

    /**
     * @see AbstractSqlEncoder#encodeNullSafe(PreparedStatement, int, Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Date value) throws SQLException
    {
        java.sql.Date date = new java.sql.Date(value.getTime());

        preparedStatement.setDate(parameterIndex, date);
    }
}
