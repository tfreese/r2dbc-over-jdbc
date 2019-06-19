/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * @author Thomas Freese
 */
public class LocalTimeConverter extends AbstractConverter<LocalTime>
{
    /**
     * Erstellt ein neues {@link LocalTimeConverter} Object.
     */
    public LocalTimeConverter()
    {
        super(LocalTime.class);
    }

    /**
     * @see AbstractConverter#doConvertNullSafe(Object)
     */
    @Override
    protected LocalTime doConvertNullSafe(final Object value)
    {
        if (value instanceof java.sql.Date)
        {
            java.sql.Date date = (java.sql.Date) value;

            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalTime();
        }
        else if (value instanceof java.util.Date)
        {
            java.util.Date date = (java.util.Date) value;

            return LocalTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        }
        else if (value instanceof String)
        {
            return LocalTime.parse((String) value);
        }

        throw createCanNotConvertException(value);
    }
}
