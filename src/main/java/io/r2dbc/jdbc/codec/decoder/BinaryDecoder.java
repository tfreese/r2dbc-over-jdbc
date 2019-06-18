/**
 * Created: 17.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;

/**
 * @author Thomas Freese
 */
public class BinaryDecoder extends BlobDecoder
{
    /**
     * Erstellt ein neues {@link BinaryDecoder} Object.
     */
    public BinaryDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.BlobDecoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.BINARY.getVendorTypeNumber();
    }
}
