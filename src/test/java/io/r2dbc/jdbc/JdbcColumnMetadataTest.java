package io.r2dbc.jdbc;

import static io.r2dbc.spi.Nullability.NULLABLE;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import java.sql.JDBCType;
import org.junit.jupiter.api.Test;

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
        assertThatNullPointerException().isThrownBy(() -> new JdbcColumnMetadata(null, JDBCType.OTHER.getVendorTypeNumber(), NULLABLE, 100, 500))
                .withMessage("name must not be null");
    }

    /**
     *
     */
    @Test
    void constructorNoNullability()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcColumnMetadata("test-name", JDBCType.OTHER.getVendorTypeNumber(), null, 100, 500))
                .withMessage("nullability must not be null");
    }
}