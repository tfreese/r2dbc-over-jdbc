/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.converter.transformer;

import java.lang.reflect.ParameterizedType;
import java.util.Objects;
import io.r2dbc.spi.Row;

/**
 * Basic-Implementation for an {@link ObjectTransformer}.
 *
 * @see Row#get(int, Class)
 * @see Row#get(String, Class)
 * @author Thomas Freese
 * @param <T> Type
 */
public abstract class AbstractObjectTransofrmer<T> implements ObjectTransformer<T>
{
    /**
    *
    */
    private final Class<T> javaType;

    /**
     * Erstellt ein neues {@link AbstractObjectTransofrmer} Object.
     */
    @SuppressWarnings("unchecked")
    public AbstractObjectTransofrmer()
    {
        super();

        this.javaType = (Class<T>) ((ParameterizedType) (getClass().getGenericSuperclass())).getActualTypeArguments()[0];
    }

    /**
     * Erstellt ein neues {@link AbstractObjectTransofrmer} Object.
     *
     * @param javaType {@link Class}
     */
    protected AbstractObjectTransofrmer(final Class<T> javaType)
    {
        super();

        this.javaType = Objects.requireNonNull(javaType, "javaType required");
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
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#getJavaType()
     */
    @Override
    public Class<T> getJavaType()
    {
        return this.javaType;
    }
}
