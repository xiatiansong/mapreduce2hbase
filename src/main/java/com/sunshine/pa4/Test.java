package com.sunshine.pa4;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import org.apache.hadoop.io.Text;
/**
 *
 * @author Ming
 */
public class Test {

    public static void main(String[] args) {
	Text t = new Text(" ab");
	String[] s = t.toString().split(" ");
	System.out.println("" + s.length + " :" + s[0] + ": ");
    }
}
