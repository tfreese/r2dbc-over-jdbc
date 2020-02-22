/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

import java.lang.reflect.ParameterizedType;
import java.util.Objects;
import io.r2dbc.spi.Row;

/**
 * Convertes an Object into another one.
 *
 * @see Row#get(int, Class)
 * @see Row#get(String, Class)
 * @author Thomas Freese
 * @param <T> Type
 */
public abstract class AbstractConverter<T> implements Converter<T>
{
    /**
    *
    */
    private final Class<T> javaType;

    /**
     * Erstellt ein neues {@link AbstractConverter} Object.
     */
    @SuppressWarnings("unchecked")
    public AbstractConverter()
    {
        super();

        this.javaType = (Class<T>) ((ParameterizedType) (getClass().getGenericSuperclass())).getActualTypeArguments()[0];
    }

    /**
     * Erstellt ein neues {@link AbstractConverter} Object.
     *
     * @param javaType {@link Class}
     */
    protected AbstractConverter(final Class<T> javaType)
    {
        super();

        this.javaType = Objects.requireNonNull(javaType, "javaType required");
    }

    /**
     * @see io.r2dbc.jdbc.codec.converter.Converter#convert(java.lang.Object)
     */
    @Override
    public final T convert(final Object value)
    {
        if (value == null)
        {
            return null;
        }

        return doConvertNullSafe(value);
    }

    /**
     * @param value Object
     * @return {@link RuntimeException}
     */
    protected RuntimeException createCanNotConvertException(final Object value)
    {
        return new IllegalArgumentException(String.format("can not convert %s into %s", value.getClass().getSimpleName(), getJavaType().getSimpleName()));
    }

    /**
     * @param value Object
     * @return Object
     */
    protected abstract T doConvertNullSafe(Object value);

    /**
     * @see io.r2dbc.jdbc.codec.converter.Converter#getJavaType()
     */
    @Override
    public Class<T> getJavaType()
    {
        return this.javaType;
    }
}
