// Created: 14.06.2019
package io.r2dbc.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * @author Thomas Freese
 */
final class JdbcConnectionFactoryMetadataTest {
    @Test
    void testName() {
        assertThat(JdbcConnectionFactoryMetadata.INSTANCE.getName()).isEqualTo(JdbcConnectionFactoryMetadata.NAME);
    }
}
