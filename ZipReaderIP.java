package com.logAnalytics.inOutZipFormat;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class ZipReaderIP extends FileInputFormat<Text, BytesWritable> {

	

	private static boolean isLenient = false;

	// default is splittable , since we need a whole file issplittable is false
	@Override
	protected boolean isSplitable(org.apache.hadoop.mapreduce.JobContext ctx,
			Path filename) {
		return false;
	}

	public static void setLenient(boolean lenient) {
		isLenient = lenient;
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RecordReaderZIP();
	}
}