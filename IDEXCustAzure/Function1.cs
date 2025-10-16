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

        private static object SafeGetValue(SqlDataReader reader, int index)
        {
            return reader.IsDBNull(index) ? null : reader.GetValue(index);
        }

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

                string sourceConnectionString = "Server=jdeapidevdbserver.database.windows.net;Database=jdeapidev;User ID=jdeapidev;Password=Idexlc1@3;Connect Timeout=60;";
                string targetApiUrlCustomer = Environment.GetEnvironmentVariable("TargetApiUrl") ?? "http://myidexhubdevbackend.idexasia.com/api/v1/jde/customer/update/trigger";
                string targetApiUrlPayment = Environment.GetEnvironmentVariable("TargetApiUrlPayment") ?? "http://myidexhubdevbackend.idexasia.com/api/v1/jde/paymentTerm/update/trigger";

                // string sourceConnectionString = "Server=jdeapiproddbserver.database.windows.net;Database=jdeapiprod;User ID=jdeapiprodadmin;Password=Idexlc1@3;Connect Timeout=60;";
                // string targetApiUrlCustomer = Environment.GetEnvironmentVariable("TargetApiUrl") ?? "https://myidexhubprod.azurewebsites.net/api/v1/jde/customer/update/trigger";
                // string targetApiUrlPayment = Environment.GetEnvironmentVariable("TargetApiUrlPayment") ?? "https://myidexhubprod.azurewebsites.net/api/v1/jde/paymentTerm/update/trigger";

                using (SqlConnection sourceConnection = new SqlConnection(sourceConnectionString))
                {
                    await sourceConnection.OpenAsync();

                    // ------------------ JdeCustomerMaster ------------------
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
                    } // Close the reader before executing another command.

                    // Step 4: Send data to API
                    if (changedRows.Count > 0)
                    {
                        string jsonData = JsonConvert.SerializeObject(changedRows);
                        var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                        var response = await _httpClient.PostAsync(targetApiUrlCustomer, content);
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

                    // ------------------ PaymentTerms (Simple + Milestone in ONE payload) ------------------
                    try
                    {
                        // ================= PaymentTermsSimple =================
                        long lastVersionSimple = 0;
                        getLastVersionCmd = new SqlCommand("SELECT LastSyncVersion FROM SyncVersionTracker WHERE TableName = 'PaymentTermsSimple'", sourceConnection);
                        lastVersionObj = await getLastVersionCmd.ExecuteScalarAsync();
                        if (lastVersionObj != null && long.TryParse(lastVersionObj.ToString(), out parsedVersion))
                        {
                            lastVersionSimple = parsedVersion;
                        }

                        currentVersionCmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", sourceConnection);
                        var currentVersionSimpleObj = await currentVersionCmd.ExecuteScalarAsync();
                        long currentVersionSimple = (currentVersionSimpleObj == null || currentVersionSimpleObj == DBNull.Value) ? 0L : Convert.ToInt64(currentVersionSimpleObj);

                        string fetchChangesQuerySimple;
                        SqlCommand fetchCommandSimple;

                        if (lastVersionSimple == 0)
                        {
                            fetchChangesQuerySimple = @"SELECT *, 'I' AS SYS_CHANGE_OPERATION FROM dbo.PaymentTermsSimple";
                            fetchCommandSimple = new SqlCommand(fetchChangesQuerySimple, sourceConnection);
                        }
                        else
                        {
                            fetchChangesQuerySimple = @"
                                SELECT c.*, CT.SYS_CHANGE_OPERATION
                                FROM CHANGETABLE(CHANGES dbo.PaymentTermsSimple, @lastVersion) AS CT
                                JOIN dbo.PaymentTermsSimple c ON c.PaymentTermsId = CT.PaymentTermsId";
                            fetchCommandSimple = new SqlCommand(fetchChangesQuerySimple, sourceConnection);
                            fetchCommandSimple.Parameters.AddWithValue("@lastVersion", lastVersionSimple);
                        }

                        var simpleRows = new List<Dictionary<string, object>>();
                        using (var reader = await fetchCommandSimple.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var row = new Dictionary<string, object>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    row[reader.GetName(i)] = SafeGetValue(reader, i);
                                }
                                simpleRows.Add(row);
                            }
                        }
                        await fetchCommandSimple.DisposeAsync(); // dispose command properly


                        // ================= PaymentTermsMilestone =================
                        long lastVersionMilestone = 0;
                        getLastVersionCmd = new SqlCommand("SELECT LastSyncVersion FROM SyncVersionTracker WHERE TableName = 'PaymentTermsMilestone'", sourceConnection);
                        lastVersionObj = await getLastVersionCmd.ExecuteScalarAsync();
                        if (lastVersionObj != null && long.TryParse(lastVersionObj.ToString(), out parsedVersion))
                        {
                            lastVersionMilestone = parsedVersion;
                        }

                        currentVersionCmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", sourceConnection);
                        var currentVersionMilestoneObj = await currentVersionCmd.ExecuteScalarAsync();
                        long currentVersionMilestone = (currentVersionMilestoneObj == null || currentVersionMilestoneObj == DBNull.Value) ? 0L : Convert.ToInt64(currentVersionMilestoneObj);

                        string fetchChangesQueryMilestone;
                        SqlCommand fetchCommandMilestone;

                        if (lastVersionMilestone == 0)
                        {
                            fetchChangesQueryMilestone = @"SELECT *, 'I' AS SYS_CHANGE_OPERATION FROM dbo.PaymentTermsMilestone";
                            fetchCommandMilestone = new SqlCommand(fetchChangesQueryMilestone, sourceConnection);
                        }
                        else
                        {
                            fetchChangesQueryMilestone = @"
                                SELECT c.*, CT.SYS_CHANGE_OPERATION
                                FROM CHANGETABLE(CHANGES dbo.PaymentTermsMilestone, @lastVersion) AS CT
                                JOIN dbo.PaymentTermsMilestone c ON c.PaymentTermsId = CT.PaymentTermsId";
                            fetchCommandMilestone = new SqlCommand(fetchChangesQueryMilestone, sourceConnection);
                            fetchCommandMilestone.Parameters.AddWithValue("@lastVersion", lastVersionMilestone);
                        }

                        var milestoneRows = new List<Dictionary<string, object>>();
                        using (var reader = await fetchCommandMilestone.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var row = new Dictionary<string, object>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    row[reader.GetName(i)] = SafeGetValue(reader, i);
                                }
                                milestoneRows.Add(row);
                            }
                        }
                        await fetchCommandMilestone.DisposeAsync();

                        // ================= Combine & Send =================
                        if (simpleRows.Count > 0 || milestoneRows.Count > 0)
                        {
                            var combinedPayload = new
                            {
                                PaymentTermsSimple = simpleRows,
                                PaymentTermsMilestone = milestoneRows
                            };

                            string jsonData = JsonConvert.SerializeObject(combinedPayload);
                            var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                            var response = await _httpClient.PostAsync(targetApiUrlPayment, content);
                            if (response.IsSuccessStatusCode)
                            {
                                _logger.LogInformation($"[PaymentTerms] Sent {simpleRows.Count} simple and {milestoneRows.Count} milestone records.");

                                var updateVersionSimpleCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'PaymentTermsSimple'", sourceConnection);
                                updateVersionSimpleCmd.Parameters.AddWithValue("@currentVersion", currentVersionSimple);
                                await updateVersionSimpleCmd.ExecuteNonQueryAsync();

                                var updateVersionMilestoneCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'PaymentTermsMilestone'", sourceConnection);
                                updateVersionMilestoneCmd.Parameters.AddWithValue("@currentVersion", currentVersionMilestone);
                                await updateVersionMilestoneCmd.ExecuteNonQueryAsync();
                            }
                            else
                            {
                                _logger.LogError($"[PaymentTerms] Failed to send data. HTTP status: {response.StatusCode}");
                            }
                        }
                        else
                        {
                            _logger.LogInformation("[PaymentTerms] No new or updated records found.");

                            var updateVersionSimpleCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'PaymentTermsSimple'", sourceConnection);
                            updateVersionSimpleCmd.Parameters.AddWithValue("@currentVersion", currentVersionSimple);
                            await updateVersionSimpleCmd.ExecuteNonQueryAsync();

                            var updateVersionMilestoneCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'PaymentTermsMilestone'", sourceConnection);
                            updateVersionMilestoneCmd.Parameters.AddWithValue("@currentVersion", currentVersionMilestone);
                            await updateVersionMilestoneCmd.ExecuteNonQueryAsync();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,
                            $"[PaymentTerms] Error: {ex.Message}. " +
                            $"Source: {ex.Source}. " +
                            $"StackTrace: {ex.StackTrace}"
                        );
                    }

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred in the outer function execution.");
            }
        }
    }
}



