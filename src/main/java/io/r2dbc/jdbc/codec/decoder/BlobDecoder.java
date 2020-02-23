/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import io.r2dbc.jdbc.util.R2dbcUtils;
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

        Blob blob = R2dbcUtils.sqlBlobToBlob(value);

        return blob;
    }
}
