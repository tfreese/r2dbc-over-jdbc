// Created: 28.03.2021
package io.r2dbc.jdbc.codecs;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Blob;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class BlobCodec extends AbstractCodec<Blob> {
    public static final Blob NULL_BLOB = Blob.from(Mono.empty());

    public BlobCodec() {
        super(Blob.class, JDBCType.BINARY, JDBCType.BLOB);
    }

    @Override
    public Blob mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        final java.sql.Blob value = resultSet.getBlob(columnLabel);

        if (resultSet.wasNull()) {
            return NULL_BLOB;
        }

        return R2dbcUtils.sqlBlobToBlob(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M> M mapTo(final Class<M> javaType, final Blob value) {
        if (value == null) {
            return null;
        }

        if (getJavaType().equals(javaType) || Object.class.equals(javaType)) {
            // if (NULL_BLOB.equals(value))
            // {
            // return null;
            // }

            return (M) value;
        }
        else if (ByteBuffer.class.equals(javaType)) {
            return (M) R2dbcUtils.blobToByteBuffer(value);
        }
        else if (InputStream.class.isAssignableFrom(javaType)) {
            return (M) R2dbcUtils.blobToInputStream(value);
        }
        else if (byte[].class.equals(javaType)) {
            return (M) R2dbcUtils.blobToByteArray(value);
        }

        throw throwCanNotMapException(value);
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Blob value) throws SQLException {
        final java.sql.Blob blob = preparedStatement.getConnection().createBlob();

        final byte[] bytes = R2dbcUtils.blobToByteArray(value);

        blob.setBytes(1, bytes);

        preparedStatement.setBlob(parameterIndex, blob);
    }
}
