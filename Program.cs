using Npgsql;

var connString = "Host=178.250.157.184;Username=stockreader;Password=stockreader;Database=postgres";

await using var conn = new NpgsqlConnection(connString);
await conn.OpenAsync();


Share maxPriceIn2020 = GetShareByRequest(GetRequest(true, 2020), conn).Result;

Share maxPriceIn2021 = GetShareByRequest(GetRequest(true, 2021), conn).Result;

Share minPriceIn2020 = GetShareByRequest(GetRequest(false, 2020), conn).Result;

Share minPriceIn2021 = GetShareByRequest(GetRequest(false, 2021), conn).Result;

conn.Close();
conn.Dispose();

connString = "Host=localhost;Username=postgres;Password=postgres;Database=postgres";

await using var localConn = new NpgsqlConnection(connString);
await localConn.OpenAsync();

await InsertDeveloper("Daniil Shatravka", 1, localConn);
await InsertDeveloper("Pavel Mironov", 2, localConn);

await InsertShare(GetQuestion(true, 2020), maxPriceIn2020, localConn);
await InsertShare(GetQuestion(false, 2020), minPriceIn2020, localConn);
await InsertShare(GetQuestion(true, 2021), maxPriceIn2021, localConn);
await InsertShare(GetQuestion(false, 2021), minPriceIn2021, localConn);

static Share ParseStringToShare(NpgsqlDataReader? reader)
{
    if (reader == null) throw new ArgumentNullException("Reader is null");

    var name = reader.GetString(0);
    var date = reader.GetDateTime(1);
    var price = reader.GetDouble(2);
    var volume = reader.GetInt32(3);

    return new Share(name, date, price, volume);
}

static async Task<Share> GetShareByRequest(string requestString, NpgsqlConnection conn)
{

    await using var cmd = new NpgsqlCommand(requestString, conn);
    await using var reader = await cmd.ExecuteReaderAsync();

    while (await reader.ReadAsync())
    {
        var newShare = ParseStringToShare(reader);
        System.Console.WriteLine(newShare);
        return newShare;
    }

    throw new KeyNotFoundException("There are no share by this request.");
}

static string GetRequest(bool isMax, int year)
{
    string minMax = isMax ? "MAX" : "MIN";
    return $"SELECT * FROM stock_price WHERE CAST(time_utc AS VARCHAR) LIKE '{year}%' AND price = (SELECT {minMax}(price) FROM stock_price WHERE CAST(time_utc AS VARCHAR) LIKE '{year}%') LIMIT 1;";
}

static string GetQuestion(bool isMax, int year)
{
    string minMax = isMax ? "max" : "min";
    return $"What is {minMax} in {year}";
}

static async Task InsertDeveloper(string developerName, int countOfDeveloper, NpgsqlConnection conn)
{
    await using var cmd = new NpgsqlCommand($"INSERT INTO kekes (question, answer) VALUES ('Who is developer #{countOfDeveloper}', '{developerName}');", conn);

    await cmd.ExecuteNonQueryAsync();
}

static async Task InsertShare(string question, Share share, NpgsqlConnection conn)
{
    await using var cmd = new NpgsqlCommand($"INSERT INTO kekes (question, answer, time_utc) VALUES ('{question}', '{share.Price}',  TO_TIMESTAMP('{share.Date}', 'DD.MM.YYYY HH24:MI:SS'));", conn);

    await cmd.ExecuteNonQueryAsync();
}
