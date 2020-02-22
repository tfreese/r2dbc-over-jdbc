/**
 * Created: 22.02.2020
 */

package io.r2dbc.jdbc.codec.converter;

import io.r2dbc.spi.Clob;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class ClobConverter extends AbstractConverter<Clob>
{
    /**
     * Erstellt ein neues {@link ClobConverter} Object.
     */
    public ClobConverter()
    {
        super();

    }

    /**
     * @see io.r2dbc.jdbc.codec.converter.AbstractConverter#doConvertNullSafe(java.lang.Object)
     */
    @Override
    protected Clob doConvertNullSafe(final Object value)
    {
        if (value instanceof Clob)
        {
            return (Clob) value;
        }

        if (value instanceof CharSequence)
        {
            return Clob.from((Mono.just((CharSequence) value)));
        }

        throw createCanNotConvertException(value);
    }
}
