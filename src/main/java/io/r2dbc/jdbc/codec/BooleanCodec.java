/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;

/**
 * @author Thomas Freese
 */
public class BooleanCodec extends BitCodec
{
    /**
     * Erstellt ein neues {@link BooleanCodec} Object.
     */
    public BooleanCodec()
    {
        super(JDBCType.BOOLEAN.getVendorTypeNumber());
    }
}
