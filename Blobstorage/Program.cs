using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Microsoft.ServiceBus.Messaging;


namespace Blobstorage
{
    class Program
    {
        static void Main(string[] args)
        {
            ServiceBusTopicReadWrite();
            Console.ReadKey();
        }


        #region Service Bus Queue RW

        public static void ServiceBusQueueReadWrite()
        {
            string connectionstring =
                "Endpoint=sb://vndemo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=k33JdThtgSo0qGpc1t3cHS/xA/TFWryJ8wTOmcZtllw=;EntityPath=myqueue";
            QueueClient queueClient = QueueClient.CreateFromConnectionString(connectionstring);


            //Send Message
            var message = new BrokeredMessage("Hello Queue !!!");
            queueClient.Send(message);

            //Read Message
            queueClient.OnMessage(m =>
            {
                var readMessage = m.GetBody<string>();
                Console.WriteLine(readMessage);
                m.Complete();

            }, new OnMessageOptions()
            {
                AutoComplete = false
            });


        }

        #endregion

        #region Service Bus Topic RW

        public static void ServiceBusTopicReadWrite()
        {
            string connectionstring =
                "Endpoint=sb://vndemo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=k33JdThtgSo0qGpc1t3cHS/xA/TFWryJ8wTOmcZtllw=";
            var topicClient = TopicClient.CreateFromConnectionString(connectionstring, "mytopic");


            //Send Message
            var message = new BrokeredMessage("Hello Topic !!!");
            topicClient.Send(message);

            //Read Message
            var subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionstring, "mytopic","ConsoleAppRead");
            subscriptionClient.OnMessage(m =>
            {
                Console.WriteLine(m.GetBody<string>() + "From Subscription Client");
            });

        }

        #endregion

        #region Azure search

        public static async void AzSearch()
        {

            //Secrete
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();

            var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));

            var secret = await keyVaultClient.GetSecretAsync("https://seeecrete.vault.azure.net/secrets/AZsearchKey/c2198a0735fc4dce971725d6dbc58948")
                .ConfigureAwait(false);


            var searchServiceName = "adw";
            var apiKey = secret.Value;

            var searchClient = new SearchServiceClient(searchServiceName, new SearchCredentials(apiKey));
            var indexClient = searchClient.Indexes.GetClient("awdcustomerindex-index");

            SearchParameters sp = new SearchParameters() { SearchMode = SearchMode.All };
            var docs = indexClient.Documents.Search("Jean", sp);
            
                Console.WriteLine(JsonConvert.SerializeObject(docs.Results));
            
        }

        #endregion

        #region COSMOSDB

        public static async void CosmosDB()
        {
            using (var client= new DocumentClient(new Uri("https://vndocument.documents.azure.com:443/"), "Kd6TvrUgZwdvQlAYYmSoo5GZsVZDtTEgRjxjbCFIOgQMJSZ3CubPKm8a29FpEzt2hO2n1W16KgtTT725A9x3Pg=="))
            {
                Uri collectionUri= UriFactory.CreateDocumentCollectionUri("AdventureWorks", "Sales");


                //Create
                SalesData dummy = new SalesData()
                {
                    AddressLine1 = "1970 W",
                    Country = "United States",
                    Id = "1234"
                };

                Task createEmployee = client.CreateDocumentAsync(collectionUri, dummy);
                Task.WaitAll(createEmployee);

                //Display
                IList<SalesData> familyQuery = client.CreateDocumentQuery<SalesData>(collectionUri)
                    .Where(f => f.Country == "United States").ToList();

                foreach (var salesData in familyQuery)
                {
                    Console.WriteLine(salesData.AddressLine1, salesData.Country);
                }

                //Get/Delete Document ById
                var deleteDocuemnt= client.CreateDocumentQuery(collectionUri).Where(f => f.Id == "9db45c55-815d-969d-7243-f1659cdaf25e").AsEnumerable().First();
                Task deleteEmployee = client.DeleteDocumentAsync(deleteDocuemnt?.SelfLink);
                Task.WaitAll(deleteEmployee);
            }
        }


        public class SalesData
        {
            public string Id { get; set; }
            public string AddressLine1 { get; set; }
            public string City { get; set; }
            public string StateProvince { get; set; }
            public string PostalCode { get; set; }
            public string Country { get; set; }
        }

        #endregion

        #region QueueStorage

        public static void QueueCreate()
        {
            CloudStorageAccount storageAccount =CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("Storagekey"));

            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            CloudQueue queue = queueClient.GetQueueReference("tasks");

            queue.CreateIfNotExists();


            //insert
            CloudQueueMessage message = new CloudQueueMessage("Hello From Queue!!");
            queue.AddMessage(message);

            //read
            CloudQueueMessage readMessage=queue.GetMessage();
            Console.WriteLine(readMessage);

            //Delete
            queue.DeleteMessage(readMessage);

        }


        #endregion

        #region TableStorage

            public static void TableCreate()
        {
            CloudStorageAccount storageAccount=CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("Storagekey"));

            CloudTableClient tableClient=storageAccount.CreateCloudTableClient();

            CloudTable table=tableClient.GetTableReference("CustomerUS");

            table.CreateIfNotExists();

            CustomerUS customer = new CustomerUS("pat","pat@123.com");

            TableOperation insertOperation = TableOperation.Insert(customer);

            table.Execute(insertOperation);
        }


        public static void TableDelete()
        {
            CloudStorageAccount storageAccount =
                CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("Storagekey"));

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            CloudTable table = tableClient.GetTableReference("CustomerUS");

            table.CreateIfNotExists();

            CustomerUS customer = GetCustomer(table, "US", "8cd2c169-d867-4d02-8ba3-439d10c255a6");

            TableOperation deleteOperation = TableOperation.Delete(customer);

            table.Execute(deleteOperation);
        }

        public static CustomerUS GetCustomer(CloudTable table, string partitionKey, string rowKey)
        {
            TableOperation getOperation=TableOperation.Retrieve<CustomerUS>(partitionKey,rowKey);

            TableResult result=table.Execute(getOperation);

            return (CustomerUS) result.Result;
        }

        public class CustomerUS : TableEntity
        {
            public string Name { get; set; }

            public string Email { get; set; }

            public CustomerUS(string name,string email)
            {
                this.Name = name;
                this.Email = email;
                this.PartitionKey = "US";
                this.RowKey = Guid.NewGuid().ToString();}

            public CustomerUS()
            {

            }


        }




        #endregion

        #region Blobstorage

        public static void CreateBlob()
        {
            //Get storage Account
            CloudStorageAccount blobStorageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("Storagekey"));

            //Create Client
            CloudBlobClient blobClient=blobStorageAccount.CreateCloudBlobClient();

            CloudBlobContainer container=blobClient.GetContainerReference("images");

            //Create if doesnot exist
            container.CreateIfNotExists(BlobContainerPublicAccessType.Blob);


            //Upload
            CloudBlockBlob blockBlob= container.GetBlockBlobReference("image1.png");

            using (FileStream uploadStream= File.OpenRead(@"C:\Users\npati\Downloads\Vicinia\images\5MB.jpg"))
            {
                blockBlob.UploadFromStream(uploadStream);
            }

            //Get All
            var blobs= container.ListBlobs();

            foreach (var blob in blobs)
            {
                Console.WriteLine(blob.Uri);
            }


            //download

            using (FileStream downloadStream = File.OpenWrite(@"C:\Users\npati\Downloads\Vicinia\5MB.jpg"))
            {
                blockBlob.DownloadToStream(downloadStream);
            }

        }

        public static void CopyAsyncBlob()
        {
            //Get storage Account
            CloudStorageAccount blobStorageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("Storagekey"));

            //Create Client
            CloudBlobClient blobClient = blobStorageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference("images");

            //Create if doesnot exist
            container.CreateIfNotExists(BlobContainerPublicAccessType.Blob);

            CloudBlockBlob blockBlob = container.GetBlockBlobReference("image1.png");

            CloudBlockBlob blockBlobCopy = container.GetBlockBlobReference("Backup-images/5MBCopy.jpg");

            void Cb(IAsyncResult x) => Console.WriteLine("Copy Completed");

            blockBlobCopy.BeginStartCopy(blockBlob.Uri, Cb, null);

        }

        public static void MetaDataGetSet()
        {
            //Get storage Account
            CloudStorageAccount blobStorageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("Storagekey"));

            //Create Client
            CloudBlobClient blobClient = blobStorageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference("images");

            //Create if doesnot exist
            container.CreateIfNotExists(BlobContainerPublicAccessType.Blob);

            SetMetaData(container);
            GetMetaData(container);
        }


        public static void SetMetaData(CloudBlobContainer container)
        {
            container.Metadata.Clear();
            container.Metadata.Add("owner","Nikhil");
            container.Metadata["LastUpdateTime"] = DateTime.Now.ToShortDateString();
            container.SetMetadata();
        }

        public static void GetMetaData(CloudBlobContainer container)
        {
            container.FetchAttributes();

            foreach (var metadata in container.Metadata)
            {
                Console.WriteLine("Key {0},Value:{1}", metadata.Key, metadata.Value);
            }
        }

        #endregion
    }
}
