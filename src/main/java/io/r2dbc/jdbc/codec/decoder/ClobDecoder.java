/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;
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

        String string = null;

        try (BufferedReader buffer = new BufferedReader(value.getCharacterStream()))
        {
            string = buffer.lines().collect(Collectors.joining());
        }
        catch (IOException ex)
        {
            throw new SQLException(ex);
        }

        return Clob.from((Mono.just(string)));
    }
}
