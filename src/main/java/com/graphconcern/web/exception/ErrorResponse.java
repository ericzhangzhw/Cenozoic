package com.graphconcern.web.exception;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ErrorResponse implements Serializable
{
    private static final long serialVersionUID = 1L;

    private String httpStatus;
    private String message;

    public ErrorResponse()
    {
    }

    public ErrorResponse(String status, String msg)
    {
        this.httpStatus = status;
        this.message = msg;
    }

    public String getHttpStatus()
    {
        return httpStatus;
    }

    public void setHttpStatus(String httpStatus)
    {
        this.httpStatus = httpStatus;
    }

    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }
}
