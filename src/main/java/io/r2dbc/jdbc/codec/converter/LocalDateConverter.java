/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * @author Thomas Freese
 */
public class LocalDateConverter extends AbstractConverter<LocalDate>
{
    /**
     * Erstellt ein neues {@link LocalDateConverter} Object.
     */
    public LocalDateConverter()
    {
        super(LocalDate.class);
    }

    /**
     * @see AbstractConverter#doConvertNullSafe(Object)
     */
    @Override
    protected LocalDate doConvertNullSafe(final Object value)
    {
        if (value instanceof java.sql.Date)
        {
            java.sql.Date date = (java.sql.Date) value;

            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
        }
        else if (value instanceof java.util.Date)
        {
            java.util.Date date = (java.util.Date) value;

            return LocalDate.ofInstant(date.toInstant(), ZoneId.systemDefault());
        }
        else if (value instanceof String)
        {
            return LocalDate.parse((String) value);
        }

        throw createCanNotConvertException(value);
    }
}
