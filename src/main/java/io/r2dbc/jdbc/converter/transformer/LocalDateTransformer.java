/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.converter.transformer;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * @author Thomas Freese
 */
public class LocalDateTransformer extends AbstractObjectTransofrmer<LocalDate>
{
    /**
     * Erstellt ein neues {@link LocalDateTransformer} Object.
     */
    public LocalDateTransformer()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public LocalDate transform(final Object value)
    {
        if (value instanceof java.sql.Date)
        {
            java.sql.Date date = (java.sql.Date) value;

            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
        }
        else if (value instanceof java.sql.Timestamp)
        {
            java.sql.Timestamp timestamp = (java.sql.Timestamp) value;

            return timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
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
