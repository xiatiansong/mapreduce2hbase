package com.sunshine.mr2hbase;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
	/**
	 * Create the test case
	 * 
	 * @param testName
	 *            name of the test case
	 */
	public AppTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(AppTest.class);
	}

	/**
	 * Rigourous Test :-)
	 * @throws IOException 
	 */
	public void testApp() throws IOException {
		NcdcStationMetadata metadata = new NcdcStationMetadata();
		metadata.initialize(new File("E://stations-fixed-width.txt"));
		Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();

		for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
			System.out.println(entry.getKey()+ ":"+entry.getValue());
		}
	}
}
