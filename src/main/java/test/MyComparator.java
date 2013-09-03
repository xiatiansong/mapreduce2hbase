package test;

import java.util.Arrays;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

public abstract class MyComparator implements RawComparator<Text> {

	@Override
	public abstract int compare(Text o1, Text o2);

	@Override
	public final int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
			int l2) {
		int n1 = WritableUtils.decodeVIntSize(b1[s1]);
		int n2 = WritableUtils.decodeVIntSize(b2[s2]);

		byte[] _b1 = Arrays.copyOfRange(b1, s1 + n1, s1 + l1);
		byte[] _b2 = Arrays.copyOfRange(b2, s2 + n2, s2 + l2);

		String t1 = new String(_b1);
		String t2 = new String(_b2);

		return compare(new Text(t1), new Text(t2));
	}

}
