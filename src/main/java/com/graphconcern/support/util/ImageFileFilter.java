/** 
/*Graph Concern Inc Confidential Information.
/* TM and (c) 2013 Graph Concern Inc.  All Rights Reserved.
/* Reproduction in whole or in part without prior written permission of a
/* duly authorized representative is prohibited.
 * 
 * Author: Felix Chiu
 * Date: 3/20/2014
 */
package com.graphconcern.support.util;

import java.io.File;
import java.io.FileFilter;

public class ImageFileFilter implements FileFilter {
	
	@Override
	public boolean accept(File pathname) {
		return Constants.PREVIEW_IMG_TYPE.equals(getExt(pathname.getName()));
	}
	
	private String getExt(String s) {
		if (s == null) return null;
	    int i = s.lastIndexOf('.');
	    if(i>0 && i < s.length()-1) return s.substring(i+1).toLowerCase();
		return null;
	}

}
