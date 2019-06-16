package io.r2dbc.jdbc;

import static io.r2dbc.spi.Nullability.NULLABLE;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import org.junit.jupiter.api.Test;
import io.r2dbc.jdbc.codec.Codecs;

/**
 * @author Thomas Freese
 */
final class JdbcColumnMetadataTest
{
    /**
     *
     */
    @Test
    void constructorNoName()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcColumnMetadata(Codecs.FALLBACK_OBJECT_CODEC, null, NULLABLE, 100, 500))
                .withMessage("name must not be null");
    }

    /**
     *
     */
    @Test
    void constructorNoNullability()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcColumnMetadata(Codecs.FALLBACK_OBJECT_CODEC, "test-name", null, 100, 500))
                .withMessage("nullability must not be null");
    }
}