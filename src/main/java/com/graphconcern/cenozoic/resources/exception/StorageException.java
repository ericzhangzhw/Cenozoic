package com.graphconcern.cenozoic.resources.exception;

public class StorageException extends RuntimeException
{

    private static final long serialVersionUID = 1L;

    public StorageException(String msg)
    {
        super(msg);
    }
}
