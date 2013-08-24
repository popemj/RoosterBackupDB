package com.rooster.backup.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class RoosterBackupDB implements AWSCredentials
{
	private AmazonS3	_s3 = null;
	
	private String		_timeZone = null;
	private String		_dateFormatStr = null;
	private String		_backupScriptName = null;
	private String		_fileNameSuffix = null;
	private String		_bucketName = null;
	private int			_maxFilesToSave = 5;
	private String		_awsAccessKey = null;
	private String		_awsSecretKey = null;
	
	
	public RoosterBackupDB() throws FileNotFoundException, IOException
	{
		// Load Properties
		Properties prop = new Properties();
    	prop.load(new FileInputStream("/home/ec2-user/DBAdmin/backupDB.properties"));
    	_timeZone = prop.getProperty("TimeZone");
    	_dateFormatStr = prop.getProperty("DateFormat");
    	_backupScriptName = prop.getProperty("BackupScriptName");
    	_fileNameSuffix = prop.getProperty("FilenameSuffix");
    	_bucketName = prop.getProperty("BucketName");
    	_maxFilesToSave = Integer.parseInt(prop.getProperty("MaxFileToSave"));
    	_awsAccessKey = prop.getProperty("accessKey");
    	_awsSecretKey = prop.getProperty("secretKey");
    	
    	_s3 = new AmazonS3Client(this);
		_s3.setRegion(Region.getRegion(Regions.DEFAULT_REGION));
	}
	
	
	private String getFileName()
	{
		// Create filename - use US/Pacific timezone
		SimpleDateFormat sdf = new SimpleDateFormat(_dateFormatStr);
		Calendar nowCal = Calendar.getInstance(TimeZone.getTimeZone(_timeZone));
		String dateStr = sdf.format(nowCal.getTime());
		String fileName = dateStr += _fileNameSuffix;
		
		return fileName;
	}
	
	
	private void backupDB(String fileName) throws IOException, InterruptedException
	{
		// Back that thing up
		final ProcessBuilder pb = new ProcessBuilder("/bin/sh", _backupScriptName, fileName);
		pb.directory(new File("/home/ec2-user/DBAdmin"));
		final Process p = pb.start();
		p.waitFor();
		
		System.out.println("Completed backing up DB to ["+fileName+"]");
	}
	
	
	private void saveToS3(String fileName) throws FileNotFoundException
	{
		// Read file
		File file = new File(fileName);
		FileInputStream fis = new FileInputStream(file);
		
		// Add metadata
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(file.length());
		objectMetadata.setContentType("application/x-gzip");
		objectMetadata.setServerSideEncryption(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		
		// Store
		PutObjectRequest por = new PutObjectRequest(_bucketName, fileName, fis, objectMetadata);
		_s3.putObject(por);
		System.out.println("Just saved to S3 ["+_bucketName+"] --> ["+fileName+"]");
		
		file.delete();
		System.out.println("Deleted Local File ["+fileName+"]");
	}
	
	
	private void cleanUpOldies()
	{
		// Clean up past entries
		// List results are always returned in lexicographic (alphabetical) order.
		ObjectListing objectListing = _s3.listObjects(_bucketName);
		List<S3ObjectSummary> summaryList = objectListing.getObjectSummaries();
		int numExcessiveFiles = summaryList.size() - _maxFilesToSave;
		while (numExcessiveFiles > 0) {
			S3ObjectSummary deadFile = summaryList.remove(0);
			System.out.println("Deleting file on S3 ["+deadFile.getKey()+"]!");
			DeleteObjectRequest dor = new DeleteObjectRequest(_bucketName, deadFile.getKey());
			_s3.deleteObject(dor);
			--numExcessiveFiles;
		}
	}
	

	public static void main(String[] args) throws IOException, InterruptedException 
	{
		RoosterBackupDB backupDB = new RoosterBackupDB();
		String fileName = backupDB.getFileName();
		backupDB.backupDB(fileName);
		backupDB.saveToS3(fileName);
		backupDB.cleanUpOldies();
	}


	@Override
	public String getAWSAccessKeyId() {
		return _awsAccessKey;
	}


	@Override
	public String getAWSSecretKey() {
		return _awsSecretKey;
	}
}
