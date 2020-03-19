/**
 * Created: 19.03.2020
 */

package io.r2dbc.jdbc.converter.sql;

import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Blob;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class BlobSqlMapper extends AbstractSqlMapper<Blob>
{
    /**
     * Erstellt ein neues {@link BlobSqlMapper} Object.
     */
    public BlobSqlMapper()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Blob mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Blob value = resultSet.getBlob(columnLabel);

        if (resultSet.wasNull())
        {
            return Blob.from(Mono.empty());
        }

        Blob blob = R2dbcUtils.sqlBlobToBlob(value);

        return blob;
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapToSql(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @SuppressWarnings("resource")
    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Blob value) throws SQLException
    {
        java.sql.Blob blob = preparedStatement.getConnection().createBlob();

        ByteBuffer byteBuffer = R2dbcUtils.blobToByteBuffer(value);

        blob.setBytes(1, byteBuffer.array());

        preparedStatement.setBlob(parameterIndex, blob);
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#getSupportedJdbcTypes()
     */
    @Override
    public Set<JDBCType> getSupportedJdbcTypes()
    {
        return Set.of(JDBCType.BINARY, JDBCType.BLOB);
    }
}
