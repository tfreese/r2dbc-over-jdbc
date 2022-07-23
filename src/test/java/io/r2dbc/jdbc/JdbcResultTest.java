// Created: 14.06.2019
package io.r2dbc.jdbc;

import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
final class JdbcResultTest
{
    /**
     *
     */
    @Test
    void testConstructorNoRowMetadata()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcResult(Flux.empty(), null, Mono.empty())).withMessage("rowMetadata must not be null");
    }

    /**
     *
     */
    @Test
    void testConstructorNoRows()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcResult(null, Mono.empty(), Mono.empty())).withMessage("rows must not be null");
    }

    /**
     *
     */
    @Test
    void testConstructorNoRowsUpdated()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcResult(Flux.empty(), Mono.empty(), null)).withMessage("rowsUpdated must not be null");
    }

    /**
     *
     */
    @Test
    void testToResultErrorResponse()
    {
        Result result = mock(JdbcResult.class, RETURNS_SMART_NULLS);

        when(result.map(any())).thenAnswer(arg ->
                Flux.error(new SQLIntegrityConstraintViolationException("can't do something", "some state", 999)).onErrorMap(SQLException.class,
                        JdbcR2dbcExceptionFactory::convert)
        );
        when(result.getRowsUpdated()).thenReturn(Mono.empty());

        Flux.from(result.map((row, rowMetadata) -> row)).as(StepVerifier::create).verifyError(R2dbcDataIntegrityViolationException.class);

        Mono.from(result.getRowsUpdated()).as(StepVerifier::create).verifyComplete();
    }
}
