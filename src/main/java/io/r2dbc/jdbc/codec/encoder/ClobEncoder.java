/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.encoder;

import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class ClobEncoder extends AbstractEncoder<Clob>
{
    /**
     * Erstellt ein neues {@link ClobEncoder} Object.
     */
    public ClobEncoder()
    {
        super(Clob.class, JDBCType.CLOB.getVendorTypeNumber());
    }

    /**
     * @see AbstractEncoder#encodeNullSafe(PreparedStatement, int, Object)
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