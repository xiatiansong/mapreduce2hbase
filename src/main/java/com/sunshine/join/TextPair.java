package com.sunshine.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair> {
	
	//productId
	private String text;
	//标识是商品表-0 还是支付表-1 
	private int id;

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(text);
		out.writeInt(id);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		text = in.readUTF();
		id = in.readInt();
	}

	@Override
	public int compareTo(TextPair o) {
		return this.text.compareTo(o.getText());
	}
	
}
