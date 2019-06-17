/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import io.r2dbc.spi.Blob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class BlobCodec extends AbstractCodec<Blob>
{
    /**
     * Erstellt ein neues {@link BlobCodec} Object.
     */
    protected BlobCodec()
    {
        this(JDBCType.BLOB.getVendorTypeNumber());
    }

    /**
     * Erstellt ein neues {@link AbstractCodec} Object.
     *
     * @param sqlType int, {@link Types}
     */
    protected BlobCodec(final int sqlType)
    {
        super(Blob.class, sqlType);
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Blob doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Blob value = resultSet.getBlob(columnLabel);

        ByteBuffer byteBuffer = null;

        try (InputStream in = new BufferedInputStream(value.getBinaryStream()))
        {
            byte[] bytes = in.readAllBytes();

            byteBuffer = ByteBuffer.wrap(bytes);
        }
        catch (IOException ex)
        {
            throw new SQLException(ex);
        }

        byteBuffer.flip();

        return Blob.from((Mono.just(byteBuffer)));
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
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
