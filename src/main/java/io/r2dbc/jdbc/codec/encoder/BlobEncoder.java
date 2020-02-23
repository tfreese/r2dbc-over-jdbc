/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.encoder;

import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Blob;

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

        ByteBuffer byteBuffer = R2dbcUtils.blobToByteBuffer(value);

        blob.setBytes(1, byteBuffer.array());

        preparedStatement.setBlob(parameterIndex, blob);
    }
}
