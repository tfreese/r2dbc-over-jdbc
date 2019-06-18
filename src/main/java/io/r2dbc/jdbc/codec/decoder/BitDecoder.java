/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;

/**
 * @author Thomas Freese
 */
public class BitDecoder extends BooleanDecoder
{
    /**
     * Erstellt ein neues {@link BitDecoder} Object.
     */
    public BitDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.BIT.getVendorTypeNumber();
    }
}
