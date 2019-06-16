/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.util.HashMap;
import java.util.Map;

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
        register(new CharCodec());
        register(new IntegerCodec());
        register(new VarCharCodec());

        // SQL_TO_JAVATYPE_MAP.put(JDBCType.BINARY.getVendorTypeNumber(), byte[].class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.BLOB.getVendorTypeNumber(), byte[].class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.CLOB.getVendorTypeNumber(), String.class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.DATE.getVendorTypeNumber(), LocalDate.class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.DECIMAL.getVendorTypeNumber(), Long.class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.DOUBLE.getVendorTypeNumber(), Double.class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.FLOAT.getVendorTypeNumber(), Float.class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.NUMERIC.getVendorTypeNumber(), Double.class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.SMALLINT.getVendorTypeNumber(), Short.class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.TIME.getVendorTypeNumber(), LocalTime.class);
        // SQL_TO_JAVATYPE_MAP.put(JDBCType.TIMESTAMP.getVendorTypeNumber(), LocalDateTime.class);
    }

    /**
     * @param <T> Type
     * @param javaType {@link Class}
     * @return {@link Codec}
     */
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> javaType)
    {
        // Codec<T> codec = this.codecsByJavaType.getOrDefault(javaType, FALLBACK_OBJECT_CODEC);
        Codec<T> codec = (Codec<T>) this.codecsByJavaType.get(javaType);

        if (codec == null)
        {
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
