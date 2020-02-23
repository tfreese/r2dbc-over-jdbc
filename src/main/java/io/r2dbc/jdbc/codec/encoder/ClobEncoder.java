/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.encoder;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Clob;

/**
 * @author Thomas Freese
 */
public class ClobEncoder extends AbstractSqlEncoder<Clob>
{
    /**
     * Erstellt ein neues {@link ClobEncoder} Object.
     */
    public ClobEncoder()
    {
        super(JDBCType.CLOB.getVendorTypeNumber());
    }

    /**
     * @see AbstractSqlEncoder#encodeNullSafe(PreparedStatement, int, Object)
     */
    @SuppressWarnings("resource")
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Clob value) throws SQLException
    {
        java.sql.Clob clob = preparedStatement.getConnection().createClob();

        String string = R2dbcUtils.clobToString(value);

        clob.setString(1, string);

        preparedStatement.setClob(parameterIndex, clob);
    }
}
