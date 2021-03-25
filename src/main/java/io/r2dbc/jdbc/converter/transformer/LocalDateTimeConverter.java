// Created: 14.06.2019
package io.r2dbc.jdbc.converter.transformer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @author Thomas Freese
 */
public class LocalDateTimeConverter extends AbstractObjectTransofrmer<LocalDateTime>
{
    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public LocalDateTime transform(final Object value)
    {
        if (value instanceof java.sql.Date)
        {
            java.sql.Date date = (java.sql.Date) value;

            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
        else if (value instanceof java.sql.Timestamp)
        {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) value;

            return timestamp.toLocalDateTime();
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
