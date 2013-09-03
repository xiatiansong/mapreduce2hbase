package com.sunshine.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * reduce��sortʱ��value��������
 * @author hadoop
 *
 */
public class TextIntComparator extends WritableComparator {

	public TextIntComparator(){
		super(IntPaire.class, true);//ע��comparator
	}
	
	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		IntPaire  o1 = (IntPaire)a;
		IntPaire  o2 = (IntPaire)b;
		if(!(o1.getFirstKey().equals(o2.getFirstKey()))){
			return o1.getFirstKey().compareTo(o2.getFirstKey());
		}else {
			return o1.getSecondKey() - o2.getSecondKey();
		}
	}

}
