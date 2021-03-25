// Created: 14.06.2019
package io.r2dbc.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.r2dbc.jdbc.util.DBServerExtension;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
final class JdbcConnectionFactoryTest
{
    /**
     *
     */
    @RegisterExtension
    static final DBServerExtension SERVER = new DBServerExtension();

    /**
     *
     */
    @Test
    void testConstructorNoConfiguration()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcConnectionFactory((DataSource) null)).withMessage("dataSource must not be null");
    }

    /**
     *
     */
    @Test
    void testCreate()
    {
        JdbcConnectionConfiguration configuration = JdbcConnectionConfiguration.builder().dataSource(SERVER.getDataSource()).build();

        new JdbcConnectionFactory(configuration).create().as(StepVerifier::create).expectNextCount(1).verifyComplete();
    }

    /**
     *
     */
    @Test
    void testGetMetadata()
    {
        JdbcConnectionConfiguration configuration = JdbcConnectionConfiguration.builder().dataSource(SERVER.getDataSource()).build();

        assertThat(new JdbcConnectionFactory(configuration).getMetadata()).isNotNull();
    }

    /**
     *
     */
    @Test
    void testOptions()
    {
        JdbcConnectionConfiguration configuration = JdbcConnectionConfiguration.builder().dataSource(SERVER.getDataSource()).build();

        assertThat(configuration.getDataSource()).isEqualTo(SERVER.getDataSource());
    }
}
