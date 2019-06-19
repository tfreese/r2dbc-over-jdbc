/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @author Thomas Freese
 */
public class LocalDateTimeConverter extends AbstractConverter<LocalDateTime>
{
    /**
     * Erstellt ein neues {@link LocalDateTimeConverter} Object.
     */
    public LocalDateTimeConverter()
    {
        super(LocalDateTime.class);
    }

    /**
     * @see AbstractConverter#doConvertNullSafe(Object)
     */
    @Override
    protected LocalDateTime doConvertNullSafe(final Object value)
    {
        if (value instanceof java.sql.Date)
        {
            java.sql.Date date = (java.sql.Date) value;

//            return new java.sql.Timestamp(date.getTime()).toLocalDateTime();
            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
        else if (value instanceof java.util.Date)
        {
            java.util.Date date = (java.util.Date) value;

            return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
        else if (value instanceof String)
        {
            return LocalDateTime.parse((String) value);
        }

        throw createCanNotConvertException(value);
    }
}
