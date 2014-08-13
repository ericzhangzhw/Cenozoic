package com.graphconcern.cenozoic.connector.exception;

import java.io.IOException;

public class ConnectorException extends IOException {
	/**
	 * Engine Exception for Usage
	 */
	private static final long serialVersionUID = 1L;

	public ConnectorException(String className, String msg) {
		super(msg+" ("+(className.contains(".")? className.substring(className.lastIndexOf('.')+1) : className)+")");
	}
	
}
