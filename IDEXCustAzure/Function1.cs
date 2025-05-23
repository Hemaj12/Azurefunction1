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
        public async Task Run([TimerTrigger("* */10 * * * *")] TimerInfo myTimer)
        {
            try
            {
                _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

                if (myTimer.ScheduleStatus is not null)
                {
                    _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
                }

                // Get connection string and target API URL from environment variables
                // string sourceConnectionString = "Server=jdeapidevdbserver.database.windows.net;Database=jdeapidev;User ID=jdeapidev;Password=Idexlc1@3;Connect Timeout=60;";
                string sourceConnectionString = "Server=jdeapiproddbserver.database.windows.net;Database=jdeapiprod;User ID=jdeapiprodadmin;Password=Idexlc1@3;Connect Timeout=60;";
                
                // string targetApiUrl = Environment.GetEnvironmentVariable("TargetApiUrl") ?? "https://myidexhubdevbackend.idexasia.com/api/v1/jde/customer/update/trigger";
                string targetApiUrl = Environment.GetEnvironmentVariable("TargetApiUrl") ?? "https://myidexhubprod.azurewebsites.net/api/v1/jde/customer/update/trigger";
                
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
                                    var allData = new List<Dictionary<string, object>>(); // List to hold all rows

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

                                            // Add the data to the list
                                            allData.Add(data);
                                        }
                                        catch (Exception ex)
                                        {
                                            _logger.LogError(ex, "Error while processing row data.");
                                        }
                                    }

                                    // Convert the list of data into JSON format
                                    string jsonData = JsonConvert.SerializeObject(allData);

                                    // Send the entire data to the target API in one request
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
