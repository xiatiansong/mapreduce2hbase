package com.sunshine.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FirstComparator extends WritableComparator {

	public FirstComparator() {
		super(TextPair.class, true);// ×¢²ácomparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		TextPair o1 = (TextPair) a;
		TextPair o2 = (TextPair) b;
		return o1.getText().compareTo(o2.getText());
	}

}
