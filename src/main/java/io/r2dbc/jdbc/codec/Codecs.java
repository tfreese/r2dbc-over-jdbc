/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;

/**
 * Encodes and decodes objects.
 *
 * @author Thomas Freese
 */
public final class Codecs
{
    /**
     *
     */
    public static final Codec<?> FALLBACK_OBJECT_CODEC = new FallbackObjectCodec();

    /**
     *
     */
    private static final Codecs INSTANCE = new Codecs();

    /**
     *
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Codecs.class);

    /**
     * @param javaType {@link Class}
     * @param <T> Type
     * @return {@link Codec}
     */
    public static <T> Codec<T> getCodec(final Class<T> javaType)
    {
        return INSTANCE.get(javaType);
    }

    /**
     * @param sqlType int
     * @param <T> Type
     * @return {@link Codec}
     */
    public static <T> Codec<T> getCodec(final int sqlType)
    {
        return INSTANCE.get(sqlType);
    }

    /**
     * @param codec {@link Codec}
     */
    public static void registerCodec(final Codec<?> codec)
    {
        INSTANCE.register(codec);
    }

    /**
     *
     */
    private final Map<Class<?>, Codec<?>> codecsByJavaType = new HashMap<>();

    /**
     *
     */
    private final Map<Integer, Codec<?>> codecsBySqlType = new HashMap<>();

    /**
     * Erstellt ein neues {@link Codecs} Object.
     */
    private Codecs()
    {
        super();

        register(new BigIntCodec());
        register(new BitCodec());
        register(new BooleanCodec());
        register(new BinaryCodec());
        register(new BlobCodec());
        register(new CharCodec());
        register(new ClobCodec());
        register(new DateCodec());
        register(new DecimalCodec());
        register(new DoubleCodec());
        register(new FloatCodec());
        register(new IntegerCodec());
        register(new NumericCodec());
        register(new SmallIntCodec());
        register(new TimeCodec());
        register(new TimestampCodec());
        register(new VarCharCodec());
    }

    /**
     * @param <T> Type
     * @param javaType {@link Class}
     * @return {@link Codec}
     */
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> javaType)
    {
        // javaType.isAssignableFrom(this.type)
        // this.type.isInstance(value)

        // Codec<T> codec = this.codecsByJavaType.getOrDefault(javaType, FALLBACK_OBJECT_CODEC);
        Codec<T> codec = (Codec<T>) this.codecsByJavaType.get(javaType);

        if ((codec == null) && Blob.class.isAssignableFrom(javaType))
        {
            codec = (Codec<T>) this.codecsByJavaType.get(Blob.class);
        }

        if ((codec == null) && Clob.class.isAssignableFrom(javaType))
        {
            codec = (Codec<T>) this.codecsByJavaType.get(Clob.class);
        }

        if (codec == null)
        {
            LOGGER.warn("Class '{}' not mapped, using {} as default", javaType, FALLBACK_OBJECT_CODEC.getClass().getSimpleName());

            codec = (Codec<T>) FALLBACK_OBJECT_CODEC;
        }

        return codec;
    }

    /**
     * @param <T> Type
     * @param sqlType int
     * @return {@link Codec}
     */
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final int sqlType)
    {
        // Codec<T> codec = this.codecsByJavaType.codecsBySqlType(sqlType, FALLBACK_OBJECT_CODEC);
        Codec<T> codec = (Codec<T>) this.codecsBySqlType.get(sqlType);

        if (codec == null)
        {
            LOGGER.warn("sqlType '{}/{}' not mapped, using {} as default", sqlType, JDBCType.valueOf(sqlType).getName(),
                    FALLBACK_OBJECT_CODEC.getClass().getSimpleName());

            codec = (Codec<T>) FALLBACK_OBJECT_CODEC;
        }

        return codec;
    }

    /**
     * @param codec {@link Codec}
     */
    public void register(final Codec<?> codec)
    {
        this.codecsByJavaType.put(codec.getJavaType(), codec);
        this.codecsBySqlType.put(codec.getSqlType(), codec);
    }
}
