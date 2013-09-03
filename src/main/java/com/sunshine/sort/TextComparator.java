package com.sunshine.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * reduce输入时，对key进行比较从而进行分组
 * @author hadoop
 *
 */
public class TextComparator extends WritableComparator {

	public TextComparator() {
		super(IntPaire.class, true);// 注册comparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		IntPaire o1 = (IntPaire) a;
		IntPaire o2 = (IntPaire) b;
		return o1.getFirstKey().compareTo(o2.getFirstKey());
	}

}
