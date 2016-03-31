using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UpdateAzureStorage
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                string StorageAccountName = ConfigurationManager.AppSettings["StorageAccountName"];
                string StorageKeyvalue = ConfigurationManager.AppSettings["StorageAccountKeyValue"];
                StorageCredentials creds = new StorageCredentials(StorageAccountName, StorageKeyvalue);
                string fileName = Path.Combine(ConfigurationManager.AppSettings["FilePath"], "log" + DateTime.Now.ToString(" ddMMyyyy") + ".txt");
                string AuditfileName = Path.Combine(ConfigurationManager.AppSettings["FilePath"], "AuditLog" + DateTime.Now.ToString(" ddMMyyyy") + ".txt");

                CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);

                CloudTableClient client = account.CreateCloudTableClient();

                // Read from csv

                var reader = new StreamReader(File.OpenRead(ConfigurationManager.AppSettings["NewSyncFile"]));
                List<ContractAccountEntity> listA = new List<ContractAccountEntity>();

                while (!reader.EndOfStream)
                {
                    ContractAccountEntity ne = new ContractAccountEntity();
                    var line = reader.ReadLine();
                    var values = line.Split(',');
                    ne.RowKey = values[2];
                    ne.PartitionKey = values[0];
                    listA.Add(ne);
                }



                // Setting the UseNewSyncProcess field to true for the given CSV
                CloudTable table = client.GetTableReference(ConfigurationManager.AppSettings["TableName"]);

                int countUpdate = 0;
                int alreadyUpdated = 0;
                int notfound = 0;

                foreach (ContractAccountEntity cn in listA)
                {
                    TableQuery<ContractAccountEntity> Query = new TableQuery<ContractAccountEntity>().Where(
                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, cn.PartitionKey)
                            );



                    var ContractAccounts = table.ExecuteQuery(Query);

                    if (ContractAccounts.Any())
                    {
                        foreach (ContractAccountEntity ContractAccount in ContractAccounts)
                        {
                            countUpdate += 1;
                            ContractAccount.UseNewSyncProcess = true;
                            TableOperation operation = TableOperation.InsertOrMerge(ContractAccount);

                            // Execute the insert operation.
                            table.Execute(operation);
                            Console.WriteLine("Updated SyncProcess for NameId: " + ContractAccount.PartitionKey);
                            Log("Updated SyncProcess for NameId: " + ContractAccount.PartitionKey, fileName);
                            if (ContractAccounts.Count() > 1)
                            {
                                break;
                            }
                        }
                    }
                    else
                    {
                        notfound += 1;
                        Log("No data was found for NameId " + cn.PartitionKey, fileName);
                    }
                }
                #region Commeneted
                //// Read from csv
                //CloudTable table = client.GetTableReference("NotificationSetup");
                //var reader = new StreamReader(File.OpenRead(@"C:\Sayak\ContractAccount.csv"));
                //List<NotificationEntity> listA = new List<NotificationEntity>();

                //while (!reader.EndOfStream)
                //{
                //    NotificationEntity ne = new NotificationEntity();
                //    var line = reader.ReadLine();
                //    var values = line.Split(',');
                //    ne.RowKey = values[1];
                //    ne.PartitionKey = values[1];
                //    ne.Contract = values[1];

                //    listA.Add(ne);                    
                //}

                //// Insert table entity
                //foreach (NotificationEntity ent in listA)
                //{
                //    NotificationEntity notificationEnt = new NotificationEntity();
                //    notificationEnt.RowKey = ent.RowKey;
                //    notificationEnt.PartitionKey = ent.PartitionKey;
                //    notificationEnt.Timestamp = DateTime.UtcNow;
                //    notificationEnt.DeviceOs = "IOS";
                //    notificationEnt.Contract = ent.Contract;
                //    notificationEnt.SpendAmountLimit = 100;

                //    // Create the TableOperation that inserts the customer entity.
                //    var insertOperation = TableOperation.Insert(notificationEnt);

                //    // Execute the insert operation.
                //    table.Execute(insertOperation);
                //}

                //// Update table entity
                //CloudTable table = client.GetTableReference("SyncStatus");
                //TableQuery<SyncStatusEntity> Query = new TableQuery<SyncStatusEntity>().Where(
                //    TableQuery.GenerateFilterCondition("Status", QueryComparisons.Equal, "Queued"));

                //var syncStatus = table.ExecuteQuery(Query);

                //if (syncStatus.Any())
                //{
                //    foreach (SyncStatusEntity ent in syncStatus)
                //    {
                //        ent.Status = "Updated";
                //        ent.ProcessingTime = DateTime.UtcNow;
                //        ent.FinishedTime = DateTime.UtcNow;
                //        TableOperation operation = TableOperation.Replace(ent);

                //        // Execute the insert operation.
                //        table.Execute(operation);
                //        Console.WriteLine("Updated NameId: " + ent.PartitionKey);
                //    }
                //}
                //else
                //{
                //    Console.WriteLine("No data was found.");
                //}
                #endregion

                Console.WriteLine("Not found = " + notfound);
                Log("Not found = " + notfound, fileName);
                Console.WriteLine("Already Updated = " + alreadyUpdated);
                Log("Already Updated = " + alreadyUpdated, fileName);
                Console.WriteLine("Entries updated = " + countUpdate);
                Log("Entries updated = " + countUpdate, fileName);
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.ReadLine();

                Console.WriteLine(ex);
            }
        }
        public static void Log(string logMessage, string fileName)
        {
            
            if (!File.Exists(fileName))
            {
                File.Create(fileName).Close();
                
            }
            using (StreamWriter w = File.AppendText(fileName))
            {

                w.Write("\r\nLog Entry : ");
                w.WriteLine("{0} {1}", DateTime.Now.ToLongTimeString(),
                    DateTime.Now.ToLongDateString());
                w.WriteLine("  :");
                w.WriteLine("  :{0}", logMessage);
                w.WriteLine("-------------------------------");
                w.Close();
            }
        }
    }

    public class SyncStatusEntity : TableEntity
    {

        public string Status { get; set; }
        public string RfcErrorId { get; set; }
        public string RfcErrorNumber { get; set; }
        public string RfcErrorType { get; set; }
        public DateTimeOffset FinishedTime { get; set; }
        public DateTimeOffset ProcessingTime { get; set; }
        public DateTimeOffset QueuedTime { get; set; }
    }

    public class NotificationEntity : TableEntity
    {
        public string Contract { get; set; }
        public string DeviceOs { get; set; }
        public Int32 SpendAmountLimit { get; set; }
    }

    public class ContractAccountEntity : TableEntity
    {
        //AGL Name ID field
        //public string PartitionKey { get; set; }
        //        public string RowKey { get; set; }

        public Boolean UseNewSyncProcess { get; set; }
    }
    
}
