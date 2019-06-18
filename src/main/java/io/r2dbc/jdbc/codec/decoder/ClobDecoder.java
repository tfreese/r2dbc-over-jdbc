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
public class ClobDecoder extends AbstractDecoder<Clob>
{
    /**
     * Erstellt ein neues {@link BlobDecoder} Object.
     */
    public ClobDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#checkWasNull(java.sql.ResultSet, java.lang.Object)
     */
    @Override
    protected Clob checkWasNull(final ResultSet resultSet, final Clob value) throws SQLException
    {
        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Clob doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
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

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.CLOB.getVendorTypeNumber();
    }
}
