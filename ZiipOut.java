package com.logAnalytics.inOutZipFormat;


import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// overriding the default file output format and using the custom writable
public class ZiipOut<K, V> extends FileOutputFormat<K, V> {
	public static class ZipRecordWriter<K, V> extends
			org.apache.hadoop.mapreduce.RecordWriter<K, V> {
		private ZipOutputStream zipOut;

		public ZipRecordWriter(FSDataOutputStream fileOut) {
			zipOut = new ZipOutputStream(fileOut);
		}

		@Override
		public void write(K key, V value) throws IOException {
			String fname = null;
			if (key instanceof BytesWritable) {
				BytesWritable bk = (BytesWritable) key;
				fname = new String(bk.getBytes(), 0, bk.getLength());
			} else {
				fname = key.toString();
			}

			ZipEntry ze = new ZipEntry(fname);
			zipOut.closeEntry();
			zipOut.putNextEntry(ze);

			if (value instanceof BytesWritable) {
				zipOut.write(((BytesWritable) value).getBytes(), 0,
						((BytesWritable) value).getLength());
			} else {
				zipOut.write(value.toString().getBytes());
			}

		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {

			zipOut.finish();
			zipOut.flush();
			zipOut.close();
			// TODO Auto-generated method stub

		}

	}
	// methode call from driver call to instantiate the custom writable

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = context.getConfiguration();

		Path file = getDefaultWorkFile(context, ".zip");

		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		return new ZipRecordWriter<K, V>(fileOut);
	}
}
