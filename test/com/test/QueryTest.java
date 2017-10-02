package com.test;

import org.junit.Test;

public class QueryTest {
	
	@Test
	public void testA(){
		String str  = "java.lang.String";
		
		try {
			Class c = Class.forName(str);
				
			System.out.println(c);
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
	}

}
