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
        private readonly HttpClient _httpClient = new HttpClient();

        public Function1(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Function1>();
        }

        [Function("Function1")]
        public async Task Run([TimerTrigger("*/5 * * * * *")] TimerInfo myTimer)
        {
            try
            {
                _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

                if (myTimer.ScheduleStatus is not null)
                {
                    _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
                }

                // Get connection string and target API URL from environment variables
                string sourceConnectionString = "Server=jdeapidevdbserver.database.windows.net;Database=jdeapidev;User ID=jdeapidev;Password=Idexlc1@3;Connect Timeout=60;";

                string targetApiUrl = Environment.GetEnvironmentVariable("TargetApiUrl") ?? "https://myidexhubdevbackend.idexasia.com/api/v1/jde/customer/update/trigger";

                using (SqlConnection sourceConnection = new SqlConnection(sourceConnectionString))
                {
                    try
                    {
                        await sourceConnection.OpenAsync();
                        _logger.LogInformation("Database connection established successfully.");

                        // Updated query without the __$operation filter
                        string fetchChangesQuery = "SELECT * FROM JdeCustomerMaster"; // Adjust if needed

                        using (SqlCommand fetchCommand = new SqlCommand(fetchChangesQuery, sourceConnection))
                        {
                            try
                            {
                                using (SqlDataReader reader = await fetchCommand.ExecuteReaderAsync())
                                {
                                    while (await reader.ReadAsync())
                                    {
                                        try
                                        {
                                            // Create a dictionary to hold the row data dynamically
                                            var data = new Dictionary<string, object>();

                                            // Loop through all columns and add them to the dictionary
                                            for (int i = 0; i < reader.FieldCount; i++)
                                            {
                                                string columnName = reader.GetName(i);
                                                object columnValue = reader.GetValue(i);
                                                data[columnName] = columnValue;
                                            }

                                            // Convert the data into JSON format
                                            string jsonData = JsonConvert.SerializeObject(data);

                                            // Send the data to the target API
                                            var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                                            HttpResponseMessage response = await _httpClient.PostAsync(targetApiUrl, content);

                                            if (response.IsSuccessStatusCode)
                                            {
                                                _logger.LogInformation($"Data sent successfully: {jsonData}");
                                            }
                                            else
                                            {
                                                _logger.LogError($"Failed to send data: {response.StatusCode}");
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            _logger.LogError(ex, "Error while processing row data.");
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error executing fetch command or reading data.");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error establishing database connection.");
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
