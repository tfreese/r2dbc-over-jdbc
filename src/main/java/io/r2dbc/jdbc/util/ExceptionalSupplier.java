// Created: 16.02.2017
package io.r2dbc.jdbc.util;

import java.util.function.Supplier;

/**
 * Interface eines {@link Supplier} mit einer Exception.<br>
 *
 * @author Thomas Freese
 * @param <R> Konkreter Ergebnis-Typ
 * @param <E> Konkreter Exception-Typ
 * @see java.util.function.Supplier
 */
@FunctionalInterface
public interface ExceptionalSupplier<R, E extends Exception>
{
    /**
     * @see java.util.function.Supplier#get()
     * @return Object
     * @throws Exception Falls was schief geht.
     */
    public R get() throws E;
}