using System;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NUnit.Framework;
using Performance.Core;

namespace Azure
{
    [TestFixture]
    public class StorageTests
    {
        private const string BlobContainerName = "azurestoragetests";
        private const string TableName = "azurestoragetests";

        private string GetStorageConnectionString()
        {
            return CloudConfigurationManager.GetSetting("StorageConnectionString");
        }

        private CloudStorageAccount GetStorageAccount(string connectionString)
        {
            return CloudStorageAccount.Parse(connectionString);            
        }

        private CloudBlobClient GetCloudBlobClient(string connectionString)
        {
            return GetStorageAccount(connectionString).CreateCloudBlobClient();
        }
        
        private void SetupBasicStringBlob(CloudBlobClient blobClient, string blobName, string blobValue)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(BlobContainerName);
            container.CreateIfNotExists();

            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(blobValue ?? String.Empty)))
            {
                blockBlob.UploadFromStream(stream);
            } 
        }

        [Test]
        public void TestBasicStringBlobRead()
        {            
            const string blobValue = "TestBasicStringBlobRead";
            const string blobName = blobValue + "/" + blobValue;

            string connectionString = GetStorageConnectionString();
            CloudBlobClient blobClient = GetCloudBlobClient(connectionString);

            SetupBasicStringBlob(blobClient, blobName, blobValue);
            
            PerformanceHarness.Test(() =>
            {
                CloudBlobClient blobReadClient = GetCloudBlobClient(connectionString);
                CloudBlobContainer container = blobReadClient.GetContainerReference(BlobContainerName);
                // Including the exists check as that would normally be done
                if (container.Exists())
                {
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

                    if (blockBlob.Exists())
                    {
                        string blobContent = blockBlob.DownloadText(Encoding.UTF8);
                    }                    
                }                                
            }, "Read block blob including client creation", 1000);
        }

        private class TestEntity : TableEntity
        {
            public TestEntity(string partitionKey, string rowKey)
            {
                PartitionKey = partitionKey;
                RowKey = rowKey;
            }

            public TestEntity() { }

            public string EntityValue { get; set; }
        }

        private CloudTableClient GetCloudTableClient(string connectionString)
        {
            return GetStorageAccount(connectionString).CreateCloudTableClient();
        }

        private void SetupBasicStringTable(CloudTableClient tableClient, string tableName, string partitionKey, string rowKey, string entityValue)
        {
            CloudTable table = tableClient.GetTableReference(TableName);
            table.CreateIfNotExists();

            TableOperation retrieveOperation = TableOperation.Retrieve<TestEntity>(partitionKey, rowKey);
            TableResult retrievedResult = table.Execute(retrieveOperation);

            if (retrievedResult.Result == null)
            {
                var testEntity = new TestEntity(partitionKey, rowKey)
                {
                    EntityValue = entityValue
                };

                TableOperation insertOperation = TableOperation.Insert(testEntity);
                table.Execute(insertOperation);
            }
        }

        [Test]
        public void TestBasicStringTableRead()
        {
            const string tableValue = "TestBasicStringTableRead";

            string connectionString = GetStorageConnectionString();
            CloudTableClient tableClient = GetCloudTableClient(connectionString);

            SetupBasicStringTable(tableClient, TableName, tableValue, tableValue, tableValue);

            PerformanceHarness.Test(() =>
            {
                CloudTableClient tableReadClient = GetCloudTableClient(connectionString);
                CloudTable table = tableReadClient.GetTableReference(TableName);                
                // Including the exists check as that would normally be done
                if (table.Exists())
                {
                    TableOperation retrieveOperation = TableOperation.Retrieve<TestEntity>(tableValue, tableValue);
                    TableResult retrievedResult = table.Execute(retrieveOperation);

                    if (retrievedResult.Result != null)
                    {
                        string tableContent = ((TestEntity) retrievedResult.Result).EntityValue;
                    }
                }
            }, "Read table value including client creation", 1000);
        }
    }
}
