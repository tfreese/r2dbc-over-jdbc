/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.converter.transformer;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * @author Thomas Freese
 */
public class DateTransformer extends AbstractObjectTransofrmer<Date>
{
    /**
     * Erstellt ein neues {@link DateTransformer} Object.
     */
    public DateTransformer()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public Date transform(final Object value)
    {
        if (value instanceof java.sql.Date)
        {
            return new Date(((java.sql.Date) value).getTime());
        }
        else if (value instanceof java.sql.Timestamp)
        {
            return new Date(((java.sql.Timestamp) value).getTime());
        }
        else if (value instanceof String)
        {
            try
            {
                return DateFormat.getInstance().parse((String) value);
            }
            catch (ParseException pex)
            {
                throw new RuntimeException(pex);
            }
        }

        throw createCanNotConvertException(value);
    }
}
