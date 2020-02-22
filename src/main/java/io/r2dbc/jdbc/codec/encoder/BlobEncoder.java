/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.encoder;

import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import io.r2dbc.spi.Blob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class BlobEncoder extends AbstractSqlEncoder<Blob>
{
    /**
     * Erstellt ein neues {@link BlobEncoder} Object.
     */
    public BlobEncoder()
    {
        super(JDBCType.BLOB.getVendorTypeNumber());
    }

    /**
     * @see AbstractSqlEncoder#encodeNullSafe(PreparedStatement, int, Object)
     */
    @SuppressWarnings("resource")
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Blob value) throws SQLException
    {
        java.sql.Blob blob = preparedStatement.getConnection().createBlob();

        // @formatter:off
        ByteBuffer byteBuffer = Flux.from(value.stream())
                .reduce(ByteBuffer::put)
                .concatWith(Mono.from(value.discard())
                        .then(Mono.empty())
                        )
                .blockFirst();
        // @formatter:on

        byteBuffer.flip();

        blob.setBytes(1, byteBuffer.array());

        preparedStatement.setBlob(parameterIndex, blob);
    }
}
