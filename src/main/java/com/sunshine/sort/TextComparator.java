package com.sunshine.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * reduce����ʱ����key���бȽϴӶ����з���
 * @author hadoop
 *
 */
public class TextComparator extends WritableComparator {

	public TextComparator() {
		super(IntPaire.class, true);// ע��comparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		IntPaire o1 = (IntPaire) a;
		IntPaire o2 = (IntPaire) b;
		return o1.getFirstKey().compareTo(o2.getFirstKey());
	}

}
