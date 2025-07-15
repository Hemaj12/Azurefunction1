using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace IDEXCustAzure
{
    public class Function1
    {
        private readonly ILogger _logger;
        private readonly HttpClient _httpClient = new HttpClient
        {
            Timeout = Timeout.InfiniteTimeSpan
        };
        private const string SyncFilePath = "/tmp/lastSyncVersion.txt";

        public Function1(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Function1>();
        }

        
        [Function("Function1")]    
        public async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer)
        {
            try
            {
                _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

                if (myTimer.ScheduleStatus is not null)
                {
                    _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
                }

                // string sourceConnectionString = "Server=jdeapidevdbserver.database.windows.net;Database=jdeapidev;User ID=jdeapidev;Password=Idexlc1@3;Connect Timeout=60;";
                // string targetApiUrl = Environment.GetEnvironmentVariable("TargetApiUrl") ?? "https://myidexhubdevbackend.idexasia.com/api/v1/jde/customer/update/trigger";

                string sourceConnectionString = "Server=jdeapiproddbserver.database.windows.net;Database=jdeapiprod;User ID=jdeapiprodadmin;Password=Idexlc1@3;Connect Timeout=60;";
                string targetApiUrl = Environment.GetEnvironmentVariable("TargetApiUrl") ?? "https://myidexhubprod.azurewebsites.net/api/v1/jde/customer/update/trigger";

                using (SqlConnection sourceConnection = new SqlConnection(sourceConnectionString))
                {
                    await sourceConnection.OpenAsync();

                    // Step 1: Read last sync version from database
                    long lastVersion = 0;
                    var getLastVersionCmd = new SqlCommand("SELECT LastSyncVersion FROM SyncVersionTracker WHERE TableName = 'JdeCustomerMaster'", sourceConnection);
                    var lastVersionObj = await getLastVersionCmd.ExecuteScalarAsync();
                    if (lastVersionObj != null && long.TryParse(lastVersionObj.ToString(), out long parsedVersion))
                    {
                        lastVersion = parsedVersion;
                    }

                    // Step 2: Get current version
                    var currentVersionCmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", sourceConnection);
                    long currentVersion = (long)await currentVersionCmd.ExecuteScalarAsync();

                    // Step 3: Fetch changed rows since last version
                    string fetchChangesQuery = @"
                SELECT c.*, CT.SYS_CHANGE_OPERATION
                FROM CHANGETABLE(CHANGES dbo.JdeCustomerMaster, @lastVersion) AS CT
                JOIN dbo.JdeCustomerMaster c ON c.id = CT.id";

                    var fetchCommand = new SqlCommand(fetchChangesQuery, sourceConnection);
                    fetchCommand.Parameters.AddWithValue("@lastVersion", lastVersion);

                    var changedRows = new List<Dictionary<string, object>>();

                    using (var reader = await fetchCommand.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var row = new Dictionary<string, object>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                string columnName = reader.GetName(i);
                                object columnValue = reader.GetValue(i);
                                row[columnName] = columnValue;
                            }
                            changedRows.Add(row);
                        }
                    } // Close the reader before executing another command

                    // Step 4: Send data to API
                    if (changedRows.Count > 0)
                    {
                        string jsonData = JsonConvert.SerializeObject(changedRows);
                        var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                        var response = await _httpClient.PostAsync(targetApiUrl, content);
                        if (response.IsSuccessStatusCode)
                        {
                            _logger.LogInformation($"Successfully sent {changedRows.Count} records.");
                            var updateVersionCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'JdeCustomerMaster'", sourceConnection);
                            updateVersionCmd.Parameters.AddWithValue("@currentVersion", currentVersion);
                            await updateVersionCmd.ExecuteNonQueryAsync();
                        }
                        else
                        {
                            _logger.LogError($"Failed to send data. HTTP status: {response.StatusCode}");
                        }
                    }
                    else
                    {
                        _logger.LogInformation("No new or updated records found.");
                        var updateVersionCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'JdeCustomerMaster'", sourceConnection);
                        updateVersionCmd.Parameters.AddWithValue("@currentVersion", currentVersion);
                        await updateVersionCmd.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred in the function execution.");
            }
        }

    }

}
