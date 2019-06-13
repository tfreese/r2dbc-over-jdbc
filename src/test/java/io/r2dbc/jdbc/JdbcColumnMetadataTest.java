package io.r2dbc.jdbc;

import static io.r2dbc.spi.Nullability.NULLABLE;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import org.junit.jupiter.api.Test;
import io.r2dbc.jdbc.JdbcColumnMetadata;

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
        assertThatNullPointerException().isThrownBy(() -> new JdbcColumnMetadata(String.class, null, 200, NULLABLE, 100, 500))
                .withMessage("name must not be null");
    }

    /**
     *
     */
    @Test
    void constructorNoNullability()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcColumnMetadata(String.class, "test-name", 200, null, 100, 500))
                .withMessage("nullability must not be null");
    }
}