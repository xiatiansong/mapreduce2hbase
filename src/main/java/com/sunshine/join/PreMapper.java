package com.sunshine.join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PreMapper extends Mapper<Object, Text, TextPair, Text> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String path = fileSplit.getPath().toString();
		String[] line = value.toString().split("\"");
		if(line.length < 2){
			return;
		}
		TextPair tr = new TextPair();
		String productId = line[0];
		tr.setText(productId);
		Text kv = new Text();
		if(path.indexOf("action") > 0){
			//trade table
			tr.setId(0);
			//用于TextPair排序
			String tradeId = line[1];
			//value is tradeId
			kv.set(tradeId);
		}else if(path.indexOf("alipay") > 0){
			//pay table
			tr.setId(1);
			//用于TextPair排序
			String payId = line[1];
			//value is payId
			kv.set(payId);
		}
		context.write(tr, kv);
	}
}
