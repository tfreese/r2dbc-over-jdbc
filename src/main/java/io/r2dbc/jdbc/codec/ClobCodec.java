/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class ClobCodec extends AbstractCodec<Clob>
{
    /**
     * Erstellt ein neues {@link BlobCodec} Object.
     */
    protected ClobCodec()
    {
        super(Clob.class, JDBCType.CLOB.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Clob doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Clob value = resultSet.getClob(columnLabel);

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
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Clob value) throws SQLException
    {
        java.sql.Clob clob = preparedStatement.getConnection().createClob();

        // @formatter:off
        String string = Flux.from(value.stream())
                .reduce(new StringBuilder(), StringBuilder::append)
                .map(StringBuilder::toString)
                .concatWith(Mono.from(value.discard())
                        .then(Mono.empty()))
                .blockFirst();
        // @formatter:on

        clob.setString(1, string);

        preparedStatement.setClob(parameterIndex, clob);
    }
}
