package io.r2dbc.jdbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Thomas Freese
 */
final class JdbcConnectionFactoryMetadataTest
{
    /**
     *
     */
    @Test
    void name()
    {
        assertThat(JdbcConnectionFactoryMetadata.INSTANCE.getName()).isEqualTo(JdbcConnectionFactoryMetadata.NAME);
    }

}
