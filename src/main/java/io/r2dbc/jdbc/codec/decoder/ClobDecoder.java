/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class ClobDecoder extends AbstractSqlDecoder<Clob>
{
    /**
     * Erstellt ein neues {@link BlobDecoder} Object.
     */
    public ClobDecoder()
    {
        super(JDBCType.CLOB.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Clob decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Clob value = resultSet.getClob(columnLabel);

        if (resultSet.wasNull())
        {
            return Clob.from(Mono.empty());
        }

        Clob clob = R2dbcUtils.sqlClobToClob(value);

        return clob;
    }
}
