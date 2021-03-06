package com.revature.util;

/**
 * used to round a double to a given number of decimal places
 * @author Jonik
 *
 */
public class Rounder {
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}

}
