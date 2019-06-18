/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;

/**
 * @author Thomas Freese
 */
public class TimestampDecoder extends DateDecoder
{
    /**
     * Erstellt ein neues {@link TimestampDecoder} Object.
     */
    public TimestampDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.DateDecoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.TIMESTAMP.getVendorTypeNumber();
    }
}
