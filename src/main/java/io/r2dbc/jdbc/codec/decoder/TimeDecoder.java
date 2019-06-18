/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;

/**
 * @author Thomas Freese
 */
public class TimeDecoder extends DateDecoder
{
    /**
     * Erstellt ein neues {@link TimeDecoder} Object.
     */
    public TimeDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.TIME.getVendorTypeNumber();
    }
}
