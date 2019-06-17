/**
 * Created: 17.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;

/**
 * @author Thomas Freese
 */
public class BinaryCodec extends BlobCodec
{
    /**
     * Erstellt ein neues {@link BinaryCodec} Object.
     */
    public BinaryCodec()
    {
        super(JDBCType.BINARY.getVendorTypeNumber());
    }
}
