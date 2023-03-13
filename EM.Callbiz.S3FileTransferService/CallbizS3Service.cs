using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Transfer;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace EM.Callbiz.S3FileTransferService
{
    public partial class CallbizS3Service : ServiceBase
    {
        private FileSystemWatcher watcher;
        private AmazonS3Client s3Client;
        private TransferUtility transferUtility;
        private string folderToMonitor;
        private string bucketName;
        private string accessKey;
        private string secretKey;
        private string s3KeyPrefix;
        private string[] extensions;
        private bool isDeleteMode;
        private bool isVerbose;
        public CallbizS3Service()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            folderToMonitor = ConfigurationManager.AppSettings["FolderToMonitor"];
            bucketName = ConfigurationManager.AppSettings["BucketName"];
            accessKey = ConfigurationManager.AppSettings["AccessKey"];
            secretKey = ConfigurationManager.AppSettings["SecretKey"];
            s3KeyPrefix = ConfigurationManager.AppSettings["S3KeyPrefix"];
            extensions = ConfigurationManager.AppSettings["Extensions"].Split(new char[] { ',' });
            isDeleteMode = ConfigurationManager.AppSettings["IsDeleteMode"].Equals("1");
            isVerbose = ConfigurationManager.AppSettings["IsVerbose"].Equals("1");


            watcher = new FileSystemWatcher();
            watcher.Path = folderToMonitor;
            watcher.EnableRaisingEvents = true;
            watcher.Created += new FileSystemEventHandler(OnFileCreated);
            watcher.NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite | NotifyFilters.CreationTime;
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            s3Client = new AmazonS3Client(credentials, RegionEndpoint.USEast2);
            transferUtility= new TransferUtility(s3Client);
            UploadExistingFiles();
            EventLog.WriteEntry("CallbizS3Service", "Service started");

        }

        protected override void OnStop()
        {
            watcher.Dispose();
            s3Client.Dispose();
            transferUtility.Dispose();
        }
        private async void OnFileCreated(object sender, FileSystemEventArgs e)
        {
            try { 
    // Wait for the file to be completely written before uploading it
    WaitForFile(e.FullPath);

            // Add "_uploaded" suffix to the file name
            string fileName = Path.GetFileNameWithoutExtension(e.Name);
            string extension = Path.GetExtension(e.Name);
            string newFileName = $"{fileName}_uploaded{extension}";
            string[] fileInfo = fileName.Split('_');
                if ( (!fileName.EndsWith("uploaded"))&&(extensions.Any(x=> x== extension)))
                {
                    // Extract the year, month, and day from the file name
                    // int year = int.Parse(fileName.Substring(0, 4));
                    //int month = int.Parse(fileName.Substring(4, 2));
                    //int day = int.Parse(fileName.Substring(6, 2));
                    int year = int.Parse(fileInfo[2]);
                    int month = int.Parse(fileInfo[3]);
                    int day = int.Parse(fileInfo[4]);

                    // Construct the key for the S3 object
                    string key = $"{s3KeyPrefix}/{year}/{month}/{day}/{newFileName}";
                    var uploadRequest = new TransferUtilityUploadRequest
                    {
                        BucketName = bucketName,
                        Key = key,
                        FilePath = e.FullPath
                    };
                    uploadRequest.UploadProgressEvent += displayProgress;
                    // Upload the file to S3 asynchronously
                    await transferUtility.UploadAsync(uploadRequest);
                    string uploadedFilePath = Path.Combine(Path.GetDirectoryName(e.FullPath), newFileName);
                    if (isDeleteMode)
                    {
                        File.Delete(e.FullPath);
                    }
                    else
                    {
                        File.Move(e.FullPath, uploadedFilePath);
                    }
                }
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry("CallbizS3Service", ex.Message);
            }

        }
        private async void UploadExistingFiles()
        {
            try { 
            // Find all files in the folder that haven't been uploaded
            var filesToUpload = Directory.GetFiles(folderToMonitor)
                .Where(file => !file.EndsWith("_uploaded.bak")|| !file.EndsWith("_uploaded.trn"));

            // Upload each file asynchronously in parallel
            await Task.WhenAll(filesToUpload.Select(UploadFileAsync));
            }
            catch (Exception ex) {
                EventLog.WriteEntry("CallbizS3Service", ex.Message);
            }
        }
        private async Task UploadFileAsync(string filePath)
        {
            try
            {
                // Wait for the file to be completely written before uploading it
                WaitForFile(filePath);

                // Add "_uploaded" suffix to the file name
                string fileName = Path.GetFileNameWithoutExtension(filePath);
                string extension = Path.GetExtension(filePath);
                string newFileName = $"{fileName}_uploaded{extension}";
                string[] fileInfo = fileName.Split('_');
                if ((!fileName.EndsWith("uploaded")) && (extensions.Any(x => x == extension)))
                {
                    // Extract the year, month, and day from the file name
                    // int year = int.Parse(fileName.Substring(0, 4));
                    //int month = int.Parse(fileName.Substring(4, 2));
                    //int day = int.Parse(fileName.Substring(6, 2));
                    int year = int.Parse(fileInfo[2]);
                    int month = int.Parse(fileInfo[3]);
                    int day = int.Parse(fileInfo[4]);

                    // Construct the key for the S3 object
                    string key = $"{s3KeyPrefix}/{year}/{month}/{day}/{newFileName}";

                    // Upload the file to S3 asynchronously
                    using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                    {
                        var uploadRequest = new TransferUtilityUploadRequest
                        {
                            BucketName = bucketName,
                            Key = key,
                            InputStream = fileStream
                        };
                        uploadRequest.UploadProgressEvent += displayProgress;
                        await transferUtility.UploadAsync(uploadRequest);
                    }

                    // Add the "_uploaded" suffix to the file name
                    string uploadedFilePath = Path.Combine(Path.GetDirectoryName(filePath), newFileName);
                    if (isDeleteMode)
                    {
                        File.Delete(filePath);
                    }
                    else
                    {
                        File.Move(filePath, uploadedFilePath);
                    }
                }
            }
            catch (Exception ex)
            {
                EventLog.WriteEntry("CallbizS3Service", ex.Message);
            }
        }
        private void WaitForFile(string path)
        {
            while (true)
            {
                try
                {

                    FileStream fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
                    fs.Close();
                    using (var fileStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    {
                        break;
                    }
                }
                catch (IOException)
                {
                    Task.Delay(20000).Wait();
                }
            }
        }
        private void displayProgress(object sender, UploadProgressArgs args)
        {
            if (isVerbose)
            {
                if ((args.PercentDone % 10 == 0) &&(args.PercentDone > 0)) // Check if percent done is a multiple of 10
                {
                    string logMessage = string.Format("File {0} uploaded {1}% complete.", args.FilePath, args.PercentDone);

                    // Write progress update to event log
                    EventLog.WriteEntry("CallbizS3FileTransfer", logMessage, EventLogEntryType.Information);
                }
            }
        }
    }
}
