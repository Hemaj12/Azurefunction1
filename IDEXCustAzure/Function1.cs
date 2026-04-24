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

                _logger.LogInformation($"[Config] CustomerApiUrl: {targetApiUrlCustomer}");
                _logger.LogInformation($"[Config] PaymentApiUrl: {targetApiUrlPayment}");

                using (SqlConnection sourceConnection = new SqlConnection(sourceConnectionString))
                {
                    _logger.LogInformation("[DB] Attempting to open database connection...");
                    await sourceConnection.OpenAsync();
                    _logger.LogInformation("[DB] Database connection opened successfully.");

                    // ==================== JdeCustomerMaster ====================
                    _logger.LogInformation("[CustomerMaster] ════════════════════════════════════════");
                    _logger.LogInformation("[CustomerMaster] Starting sync process...");

                    long lastVersion = 0;
                    var getLastVersionCmd = new SqlCommand("SELECT LastSyncVersion FROM SyncVersionTracker WHERE TableName = 'JdeCustomerMaster'", sourceConnection);
                    var lastVersionObj = await getLastVersionCmd.ExecuteScalarAsync();
                    if (lastVersionObj != null && long.TryParse(lastVersionObj.ToString(), out long parsedVersion))
                    {
                        lastVersion = parsedVersion;
                    }
                    _logger.LogInformation($"[CustomerMaster] LastSyncVersion from tracker: {lastVersion}");

                    // Get current version
                    var currentVersionCmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", sourceConnection);
                    long currentVersion = (long)await currentVersionCmd.ExecuteScalarAsync();
                    _logger.LogInformation($"[CustomerMaster] CurrentVersion from DB: {currentVersion}");

                    // Validate min valid version
                    var minValidVersionCmd = new SqlCommand("SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.JdeCustomerMaster'))", sourceConnection);
                    var minValidObj = await minValidVersionCmd.ExecuteScalarAsync();
                    long minValidVersion = (minValidObj == null || minValidObj == DBNull.Value) ? 0L : Convert.ToInt64(minValidObj);
                    _logger.LogInformation($"[CustomerMaster] MinValidVersion: {minValidVersion}");

                    if (lastVersion < minValidVersion)
                    {
                        _logger.LogWarning($"[CustomerMaster] ⚠️ VERSION EXPIRED! LastSync={lastVersion} is behind MinValid={minValidVersion}. CHANGETABLE will return no rows.");
                    }
                    else
                    {
                        _logger.LogInformation($"[CustomerMaster] ✅ Version is valid. Gap to sync: {currentVersion - lastVersion} versions.");
                    }

                    // Fetch changed rows
                    _logger.LogInformation($"[CustomerMaster] Fetching changes since version {lastVersion}...");

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
                                object columnValue = SafeGetValue(reader, i); // fixed null safety
                                row[columnName] = columnValue;
                            }
                            changedRows.Add(row);
                        }
                    }

                    _logger.LogInformation($"[CustomerMaster] Total changed rows fetched: {changedRows.Count}");

                    if (changedRows.Count > 0)
                    {
                        // Log column names to detect schema issues
                        var columns = string.Join(", ", changedRows[0].Keys);
                        _logger.LogInformation($"[CustomerMaster] Columns being sent to API: {columns}");

                        // Log operation type breakdown
                        int ins = 0, upd = 0, del = 0;
                        foreach (var row in changedRows)
                        {
                            var op = row.ContainsKey("SYS_CHANGE_OPERATION") ? row["SYS_CHANGE_OPERATION"]?.ToString() : "?";
                            if (op == "I") ins++;
                            else if (op == "U") upd++;
                            else if (op == "D") del++;
                        }
                        _logger.LogInformation($"[CustomerMaster] Operations → Insert: {ins}, Update: {upd}, Delete: {del}");

                        string jsonData = JsonConvert.SerializeObject(changedRows);
                        _logger.LogInformation($"[CustomerMaster] Payload size: {Encoding.UTF8.GetByteCount(jsonData)} bytes");
                        _logger.LogInformation($"[CustomerMaster] Sending {changedRows.Count} records to API: {targetApiUrlCustomer}");

                        var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                        var response = await _httpClient.PostAsync(targetApiUrlCustomer, content);

                        _logger.LogInformation($"[CustomerMaster] API response status: {(int)response.StatusCode} {response.StatusCode}");

                        if (response.IsSuccessStatusCode)
                        {
                            string responseBody = await response.Content.ReadAsStringAsync();
                            _logger.LogInformation($"[CustomerMaster] ✅ Successfully sent {changedRows.Count} records.");
                            _logger.LogInformation($"[CustomerMaster] API response body: {responseBody}");

                            var updateVersionCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'JdeCustomerMaster'", sourceConnection);
                            updateVersionCmd.Parameters.AddWithValue("@currentVersion", currentVersion);
                            await updateVersionCmd.ExecuteNonQueryAsync();
                            _logger.LogInformation($"[CustomerMaster] ✅ SyncVersionTracker updated to version {currentVersion}.");
                        }
                        else
                        {
                            string responseBody = await response.Content.ReadAsStringAsync();
                            _logger.LogError($"[CustomerMaster] ❌ Failed to send data. HTTP {(int)response.StatusCode} {response.StatusCode}");
                            _logger.LogError($"[CustomerMaster] ❌ API error response body: {responseBody}");
                            _logger.LogWarning($"[CustomerMaster] ⚠️ SyncVersionTracker NOT updated. Version stays at {lastVersion}.");
                        }
                    }
                    else
                    {
                        _logger.LogInformation("[CustomerMaster] No new or updated records found. Updating version tracker anyway.");
                        var updateVersionCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'JdeCustomerMaster'", sourceConnection);
                        updateVersionCmd.Parameters.AddWithValue("@currentVersion", currentVersion);
                        await updateVersionCmd.ExecuteNonQueryAsync();
                        _logger.LogInformation($"[CustomerMaster] ✅ SyncVersionTracker updated to version {currentVersion}.");
                    }

                    // ==================== PaymentTerms ====================
                    _logger.LogInformation("[PaymentTerms] ════════════════════════════════════════");
                    _logger.LogInformation("[PaymentTerms] Starting sync process...");

                    try
                    {
                        // -------- PaymentTermsSimple --------
                        _logger.LogInformation("[PaymentTerms:Simple] Fetching last sync version...");
                        long lastVersionSimple = 0;
                        getLastVersionCmd = new SqlCommand("SELECT LastSyncVersion FROM SyncVersionTracker WHERE TableName = 'PaymentTermsSimple'", sourceConnection);
                        lastVersionObj = await getLastVersionCmd.ExecuteScalarAsync();
                        if (lastVersionObj != null && long.TryParse(lastVersionObj.ToString(), out parsedVersion))
                        {
                            lastVersionSimple = parsedVersion;
                        }
                        _logger.LogInformation($"[PaymentTerms:Simple] LastSyncVersion: {lastVersionSimple}");

                        currentVersionCmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", sourceConnection);
                        var currentVersionSimpleObj = await currentVersionCmd.ExecuteScalarAsync();
                        long currentVersionSimple = (currentVersionSimpleObj == null || currentVersionSimpleObj == DBNull.Value) ? 0L : Convert.ToInt64(currentVersionSimpleObj);
                        _logger.LogInformation($"[PaymentTerms:Simple] CurrentVersion: {currentVersionSimple}");

                        // Validate min valid version
                        var minValidSimpleCmd = new SqlCommand("SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.PaymentTermsSimple'))", sourceConnection);
                        var minValidSimpleObj = await minValidSimpleCmd.ExecuteScalarAsync();
                        long minValidSimple = (minValidSimpleObj == null || minValidSimpleObj == DBNull.Value) ? 0L : Convert.ToInt64(minValidSimpleObj);
                        _logger.LogInformation($"[PaymentTerms:Simple] MinValidVersion: {minValidSimple}");

                        if (lastVersionSimple > 0 && lastVersionSimple < minValidSimple)
                        {
                            _logger.LogWarning($"[PaymentTerms:Simple] ⚠️ VERSION EXPIRED! LastSync={lastVersionSimple} is behind MinValid={minValidSimple}.");
                        }

                        string fetchChangesQuerySimple;
                        SqlCommand fetchCommandSimple;

                        if (lastVersionSimple == 0)
                        {
                            _logger.LogInformation("[PaymentTerms:Simple] LastSyncVersion is 0 → performing full table load.");
                            fetchChangesQuerySimple = @"SELECT *, 'I' AS SYS_CHANGE_OPERATION FROM dbo.PaymentTermsSimple";
                            fetchCommandSimple = new SqlCommand(fetchChangesQuerySimple, sourceConnection);
                        }
                        else
                        {
                            _logger.LogInformation($"[PaymentTerms:Simple] Fetching changes since version {lastVersionSimple}...");
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
                        await fetchCommandSimple.DisposeAsync();

                        _logger.LogInformation($"[PaymentTerms:Simple] Rows fetched: {simpleRows.Count}");

                        if (simpleRows.Count > 0)
                        {
                            var columns = string.Join(", ", simpleRows[0].Keys);
                            _logger.LogInformation($"[PaymentTerms:Simple] Columns being sent: {columns}");

                            int ins = 0, upd = 0, del = 0;
                            foreach (var row in simpleRows)
                            {
                                var op = row.ContainsKey("SYS_CHANGE_OPERATION") ? row["SYS_CHANGE_OPERATION"]?.ToString() : "?";
                                if (op == "I") ins++;
                                else if (op == "U") upd++;
                                else if (op == "D") del++;
                            }
                            _logger.LogInformation($"[PaymentTerms:Simple] Operations → Insert: {ins}, Update: {upd}, Delete: {del}");
                        }

                        // -------- PaymentTermsMilestone --------
                        _logger.LogInformation("[PaymentTerms:Milestone] Fetching last sync version...");
                        long lastVersionMilestone = 0;
                        getLastVersionCmd = new SqlCommand("SELECT LastSyncVersion FROM SyncVersionTracker WHERE TableName = 'PaymentTermsMilestone'", sourceConnection);
                        lastVersionObj = await getLastVersionCmd.ExecuteScalarAsync();
                        if (lastVersionObj != null && long.TryParse(lastVersionObj.ToString(), out parsedVersion))
                        {
                            lastVersionMilestone = parsedVersion;
                        }
                        _logger.LogInformation($"[PaymentTerms:Milestone] LastSyncVersion: {lastVersionMilestone}");

                        currentVersionCmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", sourceConnection);
                        var currentVersionMilestoneObj = await currentVersionCmd.ExecuteScalarAsync();
                        long currentVersionMilestone = (currentVersionMilestoneObj == null || currentVersionMilestoneObj == DBNull.Value) ? 0L : Convert.ToInt64(currentVersionMilestoneObj);
                        _logger.LogInformation($"[PaymentTerms:Milestone] CurrentVersion: {currentVersionMilestone}");

                        // Validate min valid version
                        var minValidMilestoneCmd = new SqlCommand("SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.PaymentTermsMilestone'))", sourceConnection);
                        var minValidMilestoneObj = await minValidMilestoneCmd.ExecuteScalarAsync();
                        long minValidMilestone = (minValidMilestoneObj == null || minValidMilestoneObj == DBNull.Value) ? 0L : Convert.ToInt64(minValidMilestoneObj);
                        _logger.LogInformation($"[PaymentTerms:Milestone] MinValidVersion: {minValidMilestone}");

                        if (lastVersionMilestone > 0 && lastVersionMilestone < minValidMilestone)
                        {
                            _logger.LogWarning($"[PaymentTerms:Milestone] ⚠️ VERSION EXPIRED! LastSync={lastVersionMilestone} is behind MinValid={minValidMilestone}.");
                        }

                        string fetchChangesQueryMilestone;
                        SqlCommand fetchCommandMilestone;

                        if (lastVersionMilestone == 0)
                        {
                            _logger.LogInformation("[PaymentTerms:Milestone] LastSyncVersion is 0 → performing full table load.");
                            fetchChangesQueryMilestone = @"SELECT *, 'I' AS SYS_CHANGE_OPERATION FROM dbo.PaymentTermsMilestone";
                            fetchCommandMilestone = new SqlCommand(fetchChangesQueryMilestone, sourceConnection);
                        }
                        else
                        {
                            _logger.LogInformation($"[PaymentTerms:Milestone] Fetching changes since version {lastVersionMilestone}...");
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

                        _logger.LogInformation($"[PaymentTerms:Milestone] Rows fetched: {milestoneRows.Count}");

                        if (milestoneRows.Count > 0)
                        {
                            var columns = string.Join(", ", milestoneRows[0].Keys);
                            _logger.LogInformation($"[PaymentTerms:Milestone] Columns being sent: {columns}");

                            int ins = 0, upd = 0, del = 0;
                            foreach (var row in milestoneRows)
                            {
                                var op = row.ContainsKey("SYS_CHANGE_OPERATION") ? row["SYS_CHANGE_OPERATION"]?.ToString() : "?";
                                if (op == "I") ins++;
                                else if (op == "U") upd++;
                                else if (op == "D") del++;
                            }
                            _logger.LogInformation($"[PaymentTerms:Milestone] Operations → Insert: {ins}, Update: {upd}, Delete: {del}");
                        }

                        // -------- Combine & Send --------
                        if (simpleRows.Count > 0 || milestoneRows.Count > 0)
                        {
                            var combinedPayload = new
                            {
                                PaymentTermsSimple = simpleRows,
                                PaymentTermsMilestone = milestoneRows
                            };

                            string jsonData = JsonConvert.SerializeObject(combinedPayload);
                            _logger.LogInformation($"[PaymentTerms] Sending payload → Simple: {simpleRows.Count} rows, Milestone: {milestoneRows.Count} rows");
                            _logger.LogInformation($"[PaymentTerms] Payload size: {Encoding.UTF8.GetByteCount(jsonData)} bytes");
                            _logger.LogInformation($"[PaymentTerms] Sending to API: {targetApiUrlPayment}");

                            var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                            var response = await _httpClient.PostAsync(targetApiUrlPayment, content);

                            _logger.LogInformation($"[PaymentTerms] API response status: {(int)response.StatusCode} {response.StatusCode}");

                            if (response.IsSuccessStatusCode)
                            {
                                string responseBody = await response.Content.ReadAsStringAsync();
                                _logger.LogInformation($"[PaymentTerms] ✅ Successfully sent Simple: {simpleRows.Count}, Milestone: {milestoneRows.Count} records.");
                                _logger.LogInformation($"[PaymentTerms] API response body: {responseBody}");

                                var updateVersionSimpleCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'PaymentTermsSimple'", sourceConnection);
                                updateVersionSimpleCmd.Parameters.AddWithValue("@currentVersion", currentVersionSimple);
                                await updateVersionSimpleCmd.ExecuteNonQueryAsync();
                                _logger.LogInformation($"[PaymentTerms:Simple] ✅ SyncVersionTracker updated to version {currentVersionSimple}.");

                                var updateVersionMilestoneCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'PaymentTermsMilestone'", sourceConnection);
                                updateVersionMilestoneCmd.Parameters.AddWithValue("@currentVersion", currentVersionMilestone);
                                await updateVersionMilestoneCmd.ExecuteNonQueryAsync();
                                _logger.LogInformation($"[PaymentTerms:Milestone] ✅ SyncVersionTracker updated to version {currentVersionMilestone}.");
                            }
                            else
                            {
                                string responseBody = await response.Content.ReadAsStringAsync();
                                _logger.LogError($"[PaymentTerms] ❌ Failed to send data. HTTP {(int)response.StatusCode} {response.StatusCode}");
                                _logger.LogError($"[PaymentTerms] ❌ API error response body: {responseBody}");
                                _logger.LogWarning($"[PaymentTerms] ⚠️ SyncVersionTracker NOT updated due to API failure.");
                            }
                        }
                        else
                        {
                            _logger.LogInformation("[PaymentTerms] No new or updated records found. Updating version tracker anyway.");

                            var updateVersionSimpleCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'PaymentTermsSimple'", sourceConnection);
                            updateVersionSimpleCmd.Parameters.AddWithValue("@currentVersion", currentVersionSimple);
                            await updateVersionSimpleCmd.ExecuteNonQueryAsync();
                            _logger.LogInformation($"[PaymentTerms:Simple] ✅ SyncVersionTracker updated to version {currentVersionSimple}.");

                            var updateVersionMilestoneCmd = new SqlCommand("UPDATE SyncVersionTracker SET LastSyncVersion = @currentVersion WHERE TableName = 'PaymentTermsMilestone'", sourceConnection);
                            updateVersionMilestoneCmd.Parameters.AddWithValue("@currentVersion", currentVersionMilestone);
                            await updateVersionMilestoneCmd.ExecuteNonQueryAsync();
                            _logger.LogInformation($"[PaymentTerms:Milestone] ✅ SyncVersionTracker updated to version {currentVersionMilestone}.");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"[PaymentTerms] ❌ Error: {ex.Message}. Source: {ex.Source}. StackTrace: {ex.StackTrace}");
                    }

                    _logger.LogInformation("[Function] ════════════════════════════════════════");
                    _logger.LogInformation("[Function] All sync operations completed for this run.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[Function] ❌ Outer error: {ex.Message}. StackTrace: {ex.StackTrace}");
            }
        }
    }
}
