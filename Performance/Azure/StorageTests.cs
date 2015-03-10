using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.DataServices;
using NUnit.Framework;
using Performance.Core;

namespace Azure
{
    [TestFixture]
    public class StorageTests
    {
        private const string BlobContainerName = "azurestoragetests";
        private const string TableName = "azurestoragetests";

        private const int ModerateIterationCount = 1000;
        private const int HighIterationCount = 10000;

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

        private class BlobTestParameters
        {
            public string ConnectionString { get; set; }
            public string ContainerName { get; set; }
            public string BlobName { get; set; }            
        }

        private BlobTestParameters SetupBlobTestWithInitialBlob(string blobValueAndKey)
        {
            var testParameters = new BlobTestParameters
            {
                BlobName = blobValueAndKey + "/" + blobValueAndKey,
                ContainerName = BlobContainerName,
                ConnectionString = GetStorageConnectionString()
            };

            CloudBlobClient blobClient = GetCloudBlobClient(testParameters.ConnectionString);

            SetupBasicStringBlob(blobClient, testParameters.BlobName, blobValueAndKey);

            return testParameters;
        }

        private CloudBlobContainer GetBlobContainer(BlobTestParameters testParameters)
        {
            return GetBlobContainer(testParameters.ConnectionString, testParameters.ContainerName);
        }    

        private CloudBlobContainer GetBlobContainer(string connectionString, string containerName)
        {
            CloudBlobClient blobReadClient = GetCloudBlobClient(connectionString);

            return blobReadClient.GetContainerReference(containerName);     
        }

        [Test]
        public void TestBasicBlobContainerAndClientCreation()
        {
            var testParameters = new BlobTestParameters
            {
                ConnectionString = GetStorageConnectionString(),
                ContainerName = BlobContainerName
            };

            PerformanceHarness.Test(() => GetBlobContainer(testParameters),
                "Blob client creation", HighIterationCount);
        }

        [Test]
        public void TestBasicBlobExists()
        {
            BlobTestParameters testParameters = SetupBlobTestWithInitialBlob("TestBasicStringBlobExists");

            PerformanceHarness.Test(() =>
            {
                CloudBlobContainer container = GetBlobContainer(testParameters);

                container.Exists();
            }, "Check blob container exists including client creation", ModerateIterationCount);
        }

        [Test]
        public void TestBasicBlobExistsMultipleLevel()
        {            
            BlobTestParameters testParameters = SetupBlobTestWithInitialBlob("TestBasicBlobExistsMultipleLevel");

            PerformanceHarness.Test(() =>
            {
                CloudBlobContainer container = GetBlobContainer(testParameters);
                
                if (container.Exists())
                {
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(testParameters.BlobName);

                    blockBlob.Exists();
                }
            }, "Check blob container and block blob exists including client creation", ModerateIterationCount);
        }

        [Test]
        public void TestBasicStringBlobRead()
        {
            BlobTestParameters testParameters = SetupBlobTestWithInitialBlob("TestBasicStringBlobRead");            
            
            PerformanceHarness.Test(() =>
            {
                CloudBlobContainer container = GetBlobContainer(testParameters);
                
                if (container.Exists())
                {
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(testParameters.BlobName);

                    if (blockBlob.Exists())
                    {
                        blockBlob.DownloadText(Encoding.UTF8);
                    }                    
                }
            }, "Read block blob including client creation and check blob container and block blob exists", ModerateIterationCount);
        }

        [Test]
        public void TestBasicStringBlobReadAssumingExists()
        {
            BlobTestParameters testParameters = SetupBlobTestWithInitialBlob("TestBasicStringBlobRead");  

            PerformanceHarness.Test(() =>
            {
                CloudBlobContainer container = GetBlobContainer(testParameters);
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(testParameters.BlobName);
                blockBlob.DownloadText(Encoding.UTF8);
            }, "Read block blob including client creation (assuming blob exists)", ModerateIterationCount);
        }

        [Test]
        public void TestBasicStringBlobReadAssumingExistsAsynchronous()
        {
            BlobTestParameters testParameters = SetupBlobTestWithInitialBlob("TestBasicStringBlobRead");

            PerformanceHarness.TestAsync(async () =>
            {
                CloudBlobContainer container = GetBlobContainer(testParameters);
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(testParameters.BlobName);
                await blockBlob.DownloadTextAsync();                
            }, "Read block blob asynchronously including client creation (assuming blob exists)", ModerateIterationCount);
        }

        [Test]
        public void TestBasicStringBlobReadWhenDoesNotExist()
        {
            var testParameters = new BlobTestParameters
            {
                BlobName = "NonExistentBlob",
                ConnectionString = GetStorageConnectionString(),
                ContainerName = BlobContainerName
            };
            
            PerformanceHarness.Test(() =>
            {
                CloudBlobContainer container = GetBlobContainer(testParameters);
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(testParameters.BlobName);
                try
                {
                    blockBlob.DownloadText(Encoding.UTF8);
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode != (int) HttpStatusCode.NotFound)
                    {
                        throw;
                    }
                }

            }, "Fail to read block blob including client creation", ModerateIterationCount);
        }

        private void DeleteBlob(BlobTestParameters testParameters)
        {
            CloudBlobContainer blobContainer = GetBlobContainer(testParameters);
            CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(testParameters.BlobName);
            blockBlob.Delete();
        }

        [Test]
        public void TestBasicStringBlobReadFirstTime()
        {
            var testParameterList = new List<BlobTestParameters>();
            for (var i = 0; i < ModerateIterationCount; i++)
            {
                testParameterList.Add(SetupBlobTestWithInitialBlob(Guid.NewGuid().ToString()));          
            }

            var testParameterQueue = new Queue<BlobTestParameters>(testParameterList);

            try
            {
                PerformanceHarness.Test(() =>
                {
                    BlobTestParameters testParameters = testParameterQueue.Dequeue();

                    CloudBlobContainer container = GetBlobContainer(testParameters);
                    CloudBlockBlob blockBlob = container.GetBlockBlobReference(testParameters.BlobName);
                    blockBlob.DownloadText(Encoding.UTF8);
                }, "Read block blob for first time including client creation (assuming blob exists)", ModerateIterationCount, 0);
            }
            finally
            {
                foreach (var testParameter in testParameterList)
                {
                    DeleteBlob(testParameter);
                }
            }            
        }

        private class TableTestParameters
        {
            public string ConnectionString { get; set; }
            public string ContainerName { get; set; }
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
        }

        private TableTestParameters SetupTableTestWithInitialRow(string tableValueAndKey)
        {
            var testParameters = new TableTestParameters
            {
                ContainerName = TableName,
                ConnectionString = GetStorageConnectionString(),
                PartitionKey = tableValueAndKey,
                RowKey = tableValueAndKey 
            };

            CloudTableClient tableClient = GetCloudTableClient(testParameters.ConnectionString);

            SetupBasicStringTable(tableClient, testParameters.ContainerName, testParameters.PartitionKey, testParameters.RowKey, tableValueAndKey);

            return testParameters;
        }

        private CloudTable GetTable(TableTestParameters testParameters)
        {
            return GetTable(testParameters.ConnectionString, testParameters.ContainerName);
        }

        private CloudTable GetTable(string connectionString, string containerName)
        {
            CloudTableClient tableClient = GetCloudTableClient(connectionString);
            return tableClient.GetTableReference(containerName);
        }

        // ReSharper disable once UnusedMember.Local
        // ReSharper disable once UnusedAutoPropertyAccessor.Local
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
            CloudTable table = tableClient.GetTableReference(tableName);
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
        public void TestBasicTableAndClientCreation()
        {
            var testParameters = new TableTestParameters
            {
                ConnectionString = GetStorageConnectionString(),
                ContainerName = TableName
            };

            PerformanceHarness.Test(() => GetTable(testParameters),
                "Table client creation", HighIterationCount);
        }

        [Test]
        public void TestBasicTableExists()
        {
            TableTestParameters testParameters = SetupTableTestWithInitialRow("TestBasicTableExists");

            PerformanceHarness.Test(() =>
            {
                CloudTable table = GetTable(testParameters);

                table.Exists();
            }, "Check table exists including client creation", ModerateIterationCount);
        }

        [Test]
        public void TestBasicStringTableRead()
        {
            TableTestParameters testParameters = SetupTableTestWithInitialRow("TestBasicStringTableRead");

            PerformanceHarness.Test(() =>
            {
                CloudTable table = GetTable(testParameters);             
                
                if (table.Exists())
                {
                    TableOperation retrieveOperation = TableOperation.Retrieve<TestEntity>(testParameters.PartitionKey, testParameters.RowKey);
                    table.Execute(retrieveOperation);
                }
            }, "Read table record including client creation and check table exists", ModerateIterationCount);
        }

        [Test]
        public void TestBasicStringTableReadAssumingAlreadyExists()
        {
            TableTestParameters testParameters = SetupTableTestWithInitialRow("TestBasicStringTableRead");

            PerformanceHarness.Test(() =>
            {
                CloudTable table = GetTable(testParameters);

                TableOperation retrieveOperation = TableOperation.Retrieve<TestEntity>(testParameters.PartitionKey, testParameters.RowKey);
                table.Execute(retrieveOperation);
            }, "Read table record including client creation (assume record exists)", ModerateIterationCount);
        }

        [Test]
        public void TestBasicStringTableReadWhenDoesNotExist()
        {
            var testParameters = new TableTestParameters
            {                
                ConnectionString = GetStorageConnectionString(),
                ContainerName = BlobContainerName,
                PartitionKey = "NonExistentKey",
                RowKey = "NonExistentKey",
            };

            PerformanceHarness.Test(() =>
            {
                CloudTable table = GetTable(testParameters);
                TableOperation retrieveOperation = TableOperation.Retrieve<TestEntity>(testParameters.PartitionKey, testParameters.RowKey);
                TableResult result = table.Execute(retrieveOperation);
                if (result.Result != null)
                {
                    Assert.Fail();
                }                
            }, "Fail to read table record including client creation", ModerateIterationCount);
        }

        private void DeleteTableRow(TableTestParameters testParameters)
        {
            CloudTable table = GetTable(testParameters);

            TableOperation retrieveOperation = TableOperation.Retrieve<TestEntity>(testParameters.PartitionKey, testParameters.RowKey);
            TableResult result = table.Execute(retrieveOperation);

            TableOperation deleteOperation = TableOperation.Delete(result.Result as TestEntity);
            table.Execute(deleteOperation);            
        }

        [Test]
        public void TestBasicStringTableReadFirstTime()
        {
            var testParameterList = new List<TableTestParameters>();
            for (var i = 0; i < ModerateIterationCount; i++)
            {
                testParameterList.Add(SetupTableTestWithInitialRow(Guid.NewGuid().ToString()));
            }

            var testParameterQueue = new Queue<TableTestParameters>(testParameterList);

            try
            {
                PerformanceHarness.Test(() =>
                {
                    TableTestParameters testParameters = testParameterQueue.Dequeue();

                    CloudTable table = GetTable(testParameters);

                    TableOperation retrieveOperation = TableOperation.Retrieve<TestEntity>(testParameters.PartitionKey, testParameters.RowKey);
                    table.Execute(retrieveOperation);
                }, "Read table record for first time including client creation (assuming blob exists)", ModerateIterationCount, 0);
            }
            finally
            {
                foreach (var testParameter in testParameterList)
                {
                    DeleteTableRow(testParameter);
                }
            }
        }


        // ReSharper disable UnusedMember.Local
        // ReSharper disable once CSharpWarnings::CS0618
        private class TestServiceEntity : TableServiceEntity
        {            
            public string EntityValue { get; set; }
        }
        
        [Test]
        // ReSharper disable CSharpWarnings::CS0618
        public void TestBasicStringTableReadAssumingAlreadyExistsOldForm()
        {
            TableTestParameters testParameters = SetupTableTestWithInitialRow("TestBasicStringTableRead");

            PerformanceHarness.Test(() =>
            {
                CloudTableClient tableReadClient = GetCloudTableClient(testParameters.ConnectionString);
                
                TableServiceContext serviceContext = tableReadClient.GetTableServiceContext();

                // ReSharper disable once UnusedVariable
                TestServiceEntity retrievedResult = (from entity in serviceContext.CreateQuery<TestServiceEntity>(TableName)
                                                     where entity.PartitionKey == testParameters.PartitionKey && entity.RowKey == testParameters.RowKey
                                                     select entity).FirstOrDefault();                    
            }, "Read table record (old form) including client creation (assume record exists)", ModerateIterationCount);
        }
    }
}
