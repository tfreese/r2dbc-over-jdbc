// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;

/**
 * @author Thomas Freese
 * @param <T> Type
 */
abstract class AbstractNumberCodec<T extends Number> extends AbstractCodec<T>
{
    /**
     * Erstellt ein neues {@link AbstractNumberCodec} Object.
     *
     * @param javaType Class
     * @param supportedJdbcTypes {@link JDBCType}
     */
    protected AbstractNumberCodec(final Class<T> javaType, final JDBCType...supportedJdbcTypes)
    {
        super(javaType, supportedJdbcTypes);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapTo(java.lang.Class, java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <M> M mapTo(final Class<M> javaType, final T value)
    {
        if (value == null)
        {
            return null;
        }

        if (getJavaType().equals(javaType) || Object.class.equals(javaType))
        {
            return (M) value;
        }
        else if (Byte.class.equals(javaType))
        {
            return (M) (Byte) value.byteValue();
        }
        else if (Double.class.equals(javaType))
        {
            return (M) (Double) value.doubleValue();
        }
        else if (Float.class.equals(javaType))
        {
            return (M) (Float) value.floatValue();
        }
        else if (Long.class.equals(javaType))
        {
            return (M) (Long) value.longValue();
        }
        else if (Short.class.equals(javaType))
        {
            return (M) (Short) value.shortValue();
        }
        else if (CharSequence.class.isAssignableFrom(javaType))
        {
            return (M) value.toString();
        }

        throw throwCanNotMapException(value);
    }
}
