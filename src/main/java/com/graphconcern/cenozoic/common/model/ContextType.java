package com.graphconcern.cenozoic.common.model;

public enum ContextType
{
    /**
	 * Workspace for an individual or group of people that can contain multiple documents, and all associated activity.
	 */
    COLLECTION,
    
    /**
     * Single Document sharing session that can contain multiple people.  Indicates all instances where the document or
     * version of the document are shared, and a history of all versions of the document.
     */
    COLLABORATION,
    
    /**
     * Single Instance sharing session that can contain multiple people or documents for viewing purposes.
     */
    LIVE_SHARE
}
