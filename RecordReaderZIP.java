package com.logAnalytics.inOutZipFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RecordReaderZIP extends RecordReader<Text, BytesWritable> {
	private Text presentKey;
	private BytesWritable presentValue;
	private boolean isDone = false;
	private ZipInputStream ipZipformat;
	private long splitVol = 0;
	private FSDataInputStream fileStreamIP;

	private long volRead = 0;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
			throws IOException, InterruptedException {
		
		// initialising the reader
		// it overrides the default hadoop record reader
		try {
			Configuration conf = taskAttemptContext.getConfiguration();
			Path path = ((FileSplit) inputSplit).getPath();
			splitVol = ((FileSplit) inputSplit).getLength();
			FileSystem fs = path.getFileSystem(conf);
			fileStreamIP = fs.open(path);
			ipZipformat = new ZipInputStream(fileStreamIP);
		} catch (Exception e) {
			// TODO: handle exception
			/* e.printStackTrace(); */
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
// read each zip files contents example jsons
		ZipEntry incomingzfile = null;

		try {
			incomingzfile = ipZipformat.getNextEntry();

			if (incomingzfile == null) {
				isDone = true;

				ipZipformat.closeEntry();
				return false;
			}

		} catch (Exception e) {
			// TODO: handle exception
			/* e.printStackTrace(); */

		}
		
		presentKey = new Text(incomingzfile.getName());

		volRead += incomingzfile.getCompressedSize();
// conveting to byte array
		
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		byte[] Store = new byte[8192];
		try {

			while (true) {

				int readVal = ipZipformat.read(Store, 0, 8192);
				if (readVal > 0)
					bytesOut.write(Store, 0, readVal);
				else
					break;
			}

			presentValue = new BytesWritable(bytesOut.toByteArray());
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();

			return false;
		}
		return true;
	}
// readinthe file progress
	@Override
	public float getProgress() throws IOException, InterruptedException {

		float statusUpdate = 0.0f;

		// if (sizeRead <= splitSize && !isFinished ) {
		if (!isDone) {
			statusUpdate = (float) volRead / splitVol;
		} else {

			statusUpdate = 1;

		}

		return statusUpdate;
	}
// methode to read the each files in a compressed file
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {

		return presentKey;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {

		return presentValue;
	}

	@Override
	public void close() throws IOException {

		try {
			ipZipformat.close();
		} catch (Exception e) {
		}
		try {
			fileStreamIP.close();
		} catch (Exception e) {
		}
	}
}