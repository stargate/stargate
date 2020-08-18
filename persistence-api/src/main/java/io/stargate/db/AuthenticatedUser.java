package io.stargate.db;

public interface AuthenticatedUser<T>
{
    String getName();

    T getWrapped();
}
