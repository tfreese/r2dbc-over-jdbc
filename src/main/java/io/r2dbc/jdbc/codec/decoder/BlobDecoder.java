/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import io.r2dbc.spi.Blob;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class BlobDecoder extends AbstractSqlDecoder<Blob>
{
    /**
     * Erstellt ein neues {@link BlobDecoder} Object.
     */
    public BlobDecoder()
    {
        super(JDBCType.BLOB.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Blob decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Blob value = resultSet.getBlob(columnLabel);

        if (resultSet.wasNull())
        {
            return Blob.from(Mono.empty());
        }

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
}
