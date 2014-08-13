package com.graphconcern.common.model;

public enum AccountType
{
	/**
	 * An account for a real person.
	 */
    USER,
    
    /** 
     * A system account.
     */
	NONUSER,
	
	/**
	 * An account for a bot or agent.
	 */
	DIGITAL
}
