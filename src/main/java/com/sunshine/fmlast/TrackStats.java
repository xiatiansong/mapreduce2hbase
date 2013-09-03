// File generated by hadoop record compiler. Do not edit.
package com.sunshine.fmlast;

import org.apache.hadoop.record.Record;
import org.apache.hadoop.record.RecordComparator;
import org.apache.hadoop.record.meta.FieldTypeInfo;
import org.apache.hadoop.record.meta.RecordTypeInfo;
import org.apache.hadoop.record.meta.TypeID;
import org.apache.hadoop.record.meta.Utils;

public class TrackStats extends Record {
	private static final RecordTypeInfo rioRecTypeInfo;
	private static RecordTypeInfo rioRtiFilter;
	private static int[] rioRtiFilterFields;
	static {
		rioRecTypeInfo = new RecordTypeInfo("TrackStats");
		rioRecTypeInfo.addField("listeners", TypeID.IntTypeID);
		rioRecTypeInfo.addField("plays", TypeID.IntTypeID);
		rioRecTypeInfo.addField("scrobbles", TypeID.IntTypeID);
		rioRecTypeInfo.addField("radioPlays", TypeID.IntTypeID);
		rioRecTypeInfo.addField("skips", TypeID.IntTypeID);
	}
	
	/**  听这首歌的听众数 **/
	private int listeners;
	/**  播放这首歌的次数 **/
	private int plays;
	/**  收藏这首歌的次数 **/
	private int scrobbles;
	/**  电台播放这首歌的次数 **/
	private int radioPlays;
	/**  跳过这首歌的次数 **/
	private int skips;

	public TrackStats() {
	}
	
	public TrackStats(final int listeners, final int plays, final int scrobbles, final int radioPlays, final int skips) {
		this.listeners = listeners;
		this.plays = plays;
		this.scrobbles = scrobbles;
		this.radioPlays = radioPlays;
		this.skips = skips;
	}

	public static RecordTypeInfo getTypeInfo() {
		return rioRecTypeInfo;
	}

	public static void setTypeFilter(RecordTypeInfo rti) {
		if (null == rti)
			return;
		rioRtiFilter = rti;
		rioRtiFilterFields = null;
	}

	private static void setupRtiFields() {
		if (null == rioRtiFilter)
			return;
		// we may already have done this
		if (null != rioRtiFilterFields)
			return;
		int rioI, rioJ;
		rioRtiFilterFields = new int[rioRtiFilter.getFieldTypeInfos().size()];
		for (rioI = 0; rioI < rioRtiFilterFields.length; rioI++) {
			rioRtiFilterFields[rioI] = 0;
		}
		java.util.Iterator<FieldTypeInfo> _rio_itFilter = rioRtiFilter.getFieldTypeInfos().iterator();
		rioI = 0;
		while (_rio_itFilter.hasNext()) {
			FieldTypeInfo _rio_tInfoFilter = _rio_itFilter.next();
			java.util.Iterator<FieldTypeInfo> _rio_it = rioRecTypeInfo.getFieldTypeInfos().iterator();
			rioJ = 1;
			while (_rio_it.hasNext()) {
				FieldTypeInfo _rio_tInfo = _rio_it.next();
				if (_rio_tInfo.equals(_rio_tInfoFilter)) {
					rioRtiFilterFields[rioI] = rioJ;
					break;
				}
				rioJ++;
			}
			rioI++;
		}
	}

	public int getListeners() {
		return listeners;
	}

	public void setListeners(final int listeners) {
		this.listeners = listeners;
	}

	public int getPlays() {
		return plays;
	}

	public void setPlays(final int plays) {
		this.plays = plays;
	}

	public int getScrobbles() {
		return scrobbles;
	}

	public void setScrobbles(final int scrobbles) {
		this.scrobbles = scrobbles;
	}

	public int getRadioPlays() {
		return radioPlays;
	}

	public void setRadioPlays(final int radioPlays) {
		this.radioPlays = radioPlays;
	}

	public int getSkips() {
		return skips;
	}

	public void setSkips(final int skips) {
		this.skips = skips;
	}

	public void serialize(final org.apache.hadoop.record.RecordOutput _rio_a, final String _rio_tag) throws java.io.IOException {
		_rio_a.startRecord(this, _rio_tag);
		_rio_a.writeInt(listeners, "listeners");
		_rio_a.writeInt(plays, "plays");
		_rio_a.writeInt(scrobbles, "scrobbles");
		_rio_a.writeInt(radioPlays, "radioPlays");
		_rio_a.writeInt(skips, "skips");
		_rio_a.endRecord(this, _rio_tag);
	}

	private void deserializeWithoutFilter(final org.apache.hadoop.record.RecordInput _rio_a, final String _rio_tag) throws java.io.IOException {
		_rio_a.startRecord(_rio_tag);
		listeners = _rio_a.readInt("listeners");
		plays = _rio_a.readInt("plays");
		scrobbles = _rio_a.readInt("scrobbles");
		radioPlays = _rio_a.readInt("radioPlays");
		skips = _rio_a.readInt("skips");
		_rio_a.endRecord(_rio_tag);
	}

	public void deserialize(final org.apache.hadoop.record.RecordInput _rio_a, final String _rio_tag) throws java.io.IOException {
		if (null == rioRtiFilter) {
			deserializeWithoutFilter(_rio_a, _rio_tag);
			return;
		}
		// if we're here, we need to read based on version info
		_rio_a.startRecord(_rio_tag);
		setupRtiFields();
		for (int _rio_i = 0; _rio_i < rioRtiFilter.getFieldTypeInfos().size(); _rio_i++) {
			if (1 == rioRtiFilterFields[_rio_i]) {
				listeners = _rio_a.readInt("listeners");
			} else if (2 == rioRtiFilterFields[_rio_i]) {
				plays = _rio_a.readInt("plays");
			} else if (3 == rioRtiFilterFields[_rio_i]) {
				scrobbles = _rio_a.readInt("scrobbles");
			} else if (4 == rioRtiFilterFields[_rio_i]) {
				radioPlays = _rio_a.readInt("radioPlays");
			} else if (5 == rioRtiFilterFields[_rio_i]) {
				skips = _rio_a.readInt("skips");
			} else {
				java.util.ArrayList<FieldTypeInfo> typeInfos = (java.util.ArrayList<FieldTypeInfo>) (rioRtiFilter
						.getFieldTypeInfos());
				Utils.skip(_rio_a, typeInfos.get(_rio_i).getFieldID(), typeInfos.get(_rio_i).getTypeID());
			}
		}
		_rio_a.endRecord(_rio_tag);
	}

	public int compareTo(final Object _rio_peer_) throws ClassCastException {
		if (!(_rio_peer_ instanceof TrackStats)) {
			throw new ClassCastException("Comparing different types of records.");
		}
		TrackStats _rio_peer = (TrackStats) _rio_peer_;
		int _rio_ret = 0;
		_rio_ret = (listeners == _rio_peer.listeners) ? 0 : ((listeners < _rio_peer.listeners) ? -1 : 1);
		if (_rio_ret != 0)
			return _rio_ret;
		_rio_ret = (plays == _rio_peer.plays) ? 0 : ((plays < _rio_peer.plays) ? -1 : 1);
		if (_rio_ret != 0)
			return _rio_ret;
		_rio_ret = (scrobbles == _rio_peer.scrobbles) ? 0 : ((scrobbles < _rio_peer.scrobbles) ? -1 : 1);
		if (_rio_ret != 0)
			return _rio_ret;
		_rio_ret = (radioPlays == _rio_peer.radioPlays) ? 0 : ((radioPlays < _rio_peer.radioPlays) ? -1 : 1);
		if (_rio_ret != 0)
			return _rio_ret;
		_rio_ret = (skips == _rio_peer.skips) ? 0 : ((skips < _rio_peer.skips) ? -1 : 1);
		if (_rio_ret != 0)
			return _rio_ret;
		return _rio_ret;
	}

	public boolean equals(final Object _rio_peer_) {
		if (!(_rio_peer_ instanceof TrackStats)) {
			return false;
		}
		if (_rio_peer_ == this) {
			return true;
		}
		TrackStats _rio_peer = (TrackStats) _rio_peer_;
		boolean _rio_ret = false;
		_rio_ret = (listeners == _rio_peer.listeners);
		if (!_rio_ret)
			return _rio_ret;
		_rio_ret = (plays == _rio_peer.plays);
		if (!_rio_ret)
			return _rio_ret;
		_rio_ret = (scrobbles == _rio_peer.scrobbles);
		if (!_rio_ret)
			return _rio_ret;
		_rio_ret = (radioPlays == _rio_peer.radioPlays);
		if (!_rio_ret)
			return _rio_ret;
		_rio_ret = (skips == _rio_peer.skips);
		if (!_rio_ret)
			return _rio_ret;
		return _rio_ret;
	}

	public Object clone() throws CloneNotSupportedException {
		TrackStats _rio_other = new TrackStats();
		_rio_other.listeners = this.listeners;
		_rio_other.plays = this.plays;
		_rio_other.scrobbles = this.scrobbles;
		_rio_other.radioPlays = this.radioPlays;
		_rio_other.skips = this.skips;
		return _rio_other;
	}

	public int hashCode() {
		int _rio_result = 17;
		int _rio_ret;
		_rio_ret = (int) listeners;
		_rio_result = 37 * _rio_result + _rio_ret;
		_rio_ret = (int) plays;
		_rio_result = 37 * _rio_result + _rio_ret;
		_rio_ret = (int) scrobbles;
		_rio_result = 37 * _rio_result + _rio_ret;
		_rio_ret = (int) radioPlays;
		_rio_result = 37 * _rio_result + _rio_ret;
		_rio_ret = (int) skips;
		_rio_result = 37 * _rio_result + _rio_ret;
		return _rio_result;
	}

	public static String signature() {
		return "LTrackStats(iiiii)";
	}

	public static class Comparator extends org.apache.hadoop.record.RecordComparator {
		public Comparator() {
			super(TrackStats.class);
		}

		static public int slurpRaw(byte[] b, int s, int l) {
			try {
				int os = s;
				{
					int i = org.apache.hadoop.record.Utils.readVInt(b, s);
					int z = org.apache.hadoop.record.Utils.getVIntSize(i);
					s += z;
					l -= z;
				}
				{
					int i = org.apache.hadoop.record.Utils.readVInt(b, s);
					int z = org.apache.hadoop.record.Utils.getVIntSize(i);
					s += z;
					l -= z;
				}
				{
					int i = org.apache.hadoop.record.Utils.readVInt(b, s);
					int z = org.apache.hadoop.record.Utils.getVIntSize(i);
					s += z;
					l -= z;
				}
				{
					int i = org.apache.hadoop.record.Utils.readVInt(b, s);
					int z = org.apache.hadoop.record.Utils.getVIntSize(i);
					s += z;
					l -= z;
				}
				{
					int i = org.apache.hadoop.record.Utils.readVInt(b, s);
					int z = org.apache.hadoop.record.Utils.getVIntSize(i);
					s += z;
					l -= z;
				}
				return (os - s);
			} catch (java.io.IOException e) {
				throw new RuntimeException(e);
			}
		}

		static public int compareRaw(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int os1 = s1;
				{
					int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
					int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
					if (i1 != i2) {
						return ((i1 - i2) < 0) ? -1 : 0;
					}
					int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
					int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
					s1 += z1;
					s2 += z2;
					l1 -= z1;
					l2 -= z2;
				}
				{
					int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
					int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
					if (i1 != i2) {
						return ((i1 - i2) < 0) ? -1 : 0;
					}
					int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
					int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
					s1 += z1;
					s2 += z2;
					l1 -= z1;
					l2 -= z2;
				}
				{
					int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
					int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
					if (i1 != i2) {
						return ((i1 - i2) < 0) ? -1 : 0;
					}
					int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
					int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
					s1 += z1;
					s2 += z2;
					l1 -= z1;
					l2 -= z2;
				}
				{
					int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
					int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
					if (i1 != i2) {
						return ((i1 - i2) < 0) ? -1 : 0;
					}
					int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
					int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
					s1 += z1;
					s2 += z2;
					l1 -= z1;
					l2 -= z2;
				}
				{
					int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
					int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
					if (i1 != i2) {
						return ((i1 - i2) < 0) ? -1 : 0;
					}
					int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
					int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
					s1 += z1;
					s2 += z2;
					l1 -= z1;
					l2 -= z2;
				}
				return (os1 - s1);
			} catch (java.io.IOException e) {
				throw new RuntimeException(e);
			}
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int ret = compareRaw(b1, s1, l1, b2, s2, l2);
			return (ret == -1) ? -1 : ((ret == 0) ? 1 : 0);
		}
	}

	static {
		RecordComparator.define(TrackStats.class, new Comparator());
	}
}