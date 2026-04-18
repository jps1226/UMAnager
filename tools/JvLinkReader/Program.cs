using System.Reflection;
using System.Text;
using System.Text.Json;

Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

var argsMap = ParseArgs(args);
var sid = GetArg(argsMap, "sid", "UMANAGER");
var savePath = GetArg(argsMap, "save-path", "");
var dataSpec = GetArg(argsMap, "data-spec", "TOKU");
var fromDate = GetArg(argsMap, "from-date", DateTime.Today.AddDays(-7).ToString("yyyyMMdd000000"));
var serviceKey = GetArg(argsMap, "service-key", Environment.GetEnvironmentVariable("JVLINK_SERVICE_KEY") ?? "");
var skipServiceKey = HasFlag(argsMap, "skip-service-key");
var maxRecords = int.TryParse(GetArg(argsMap, "max-records", "20000"), out var mr) ? Math.Max(1, mr) : 20000;
var waitSeconds = int.TryParse(GetArg(argsMap, "wait-seconds", "180"), out var ws) ? Math.Max(1, ws) : 180;
var dataOption = int.TryParse(GetArg(argsMap, "data-option", "1"), out var dop) ? Math.Clamp(dop, 1, 3) : 1;

var result = new Result
{
    Ok = false,
    Sid = sid,
    DataSpec = dataSpec,
    FromDate = fromDate,
    SavePath = savePath,
};

try
{
    var progIdType = Type.GetTypeFromProgID("JVDTLab.JVLink", throwOnError: true)!;
    dynamic obj = Activator.CreateInstance(progIdType)!;

    result.Version = SafeString(() => obj.m_JVLinkVersion);
    result.InitCode = SafeInt(() => obj.JVInit(sid));

    if (!string.IsNullOrWhiteSpace(savePath))
    {
        Directory.CreateDirectory(savePath);
        result.SetSavePathCode = SafeInt(() => obj.JVSetSavePath(savePath));
        SafeInt(() => obj.JVSetSaveFlag(1));
    }

    if (!skipServiceKey && !string.IsNullOrWhiteSpace(serviceKey))
    {
        result.SetServiceKeyCode = SafeInt(() => obj.JVSetServiceKey(serviceKey));
    }

    int readCount = 0;
    int downloadCount = 0;
    string lastFileTimestamp = string.Empty;
    result.OpenCode = SafeInt(() => obj.JVOpen(dataSpec, fromDate, dataOption, ref readCount, ref downloadCount, ref lastFileTimestamp));
    result.ReadCount = readCount;
    result.DownloadCount = downloadCount;
    result.LastFileTimestamp = lastFileTimestamp;

    if (result.OpenCode >= 0 && downloadCount > 0)
    {
        var deadline = DateTime.UtcNow.AddSeconds(waitSeconds);
        while (DateTime.UtcNow < deadline)
        {
            result.StatusCode = SafeInt(() => obj.JVStatus());
            if (result.StatusCode < 0)
            {
                break;
            }
            if (result.StatusCode >= downloadCount)
            {
                break;
            }
            Thread.Sleep(200);
        }
    }

    var races = new Dictionary<string, RaceSnapshot>(StringComparer.OrdinalIgnoreCase);
    var recordsRead = 0;
    while (recordsRead < maxRecords)
    {
        string buff = string.Empty;
        int size = 0;
        string fileName = string.Empty;
        int ret = SafeInt(() => obj.JVRead(ref buff, ref size, ref fileName));
        if (ret <= 0 || string.IsNullOrEmpty(buff) || size <= 0)
        {
            break;
        }

        recordsRead++;
        var bytes = Encoding.GetEncoding(932).GetBytes(buff);
        if (bytes.Length < 30)
        {
            continue;
        }

        var recordSpec = GetString(bytes, 1, 2);
        if (recordSpec == "RA")
        {
            var race = ParseRa(bytes);
            if (!string.IsNullOrWhiteSpace(race.RaceId))
            {
                races[race.RaceId] = race;
            }
        }
        else if (recordSpec == "SE")
        {
            var entry = ParseSe(bytes);
            if (!string.IsNullOrWhiteSpace(entry.RaceId))
            {
                if (!races.TryGetValue(entry.RaceId, out var race))
                {
                    race = new RaceSnapshot { RaceId = entry.RaceId, Entries = new List<RaceEntry>() };
                    races[entry.RaceId] = race;
                }
                race.Entries ??= new List<RaceEntry>();
                race.Entries.Add(entry);
            }
        }
    }

    result.RecordsRead = recordsRead;
    result.Races = races.Values.OrderBy(r => r.Date).ThenBy(r => r.RaceId).ToList();
    result.Ok = true;
    result.CloseCode = SafeInt(() => obj.JVClose());
}
catch (Exception ex)
{
    result.Error = ex.ToString();
}

Console.OutputEncoding = Encoding.UTF8;
Console.WriteLine(JsonSerializer.Serialize(result, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }));

return;

static Dictionary<string, string?> ParseArgs(string[] args)
{
    var map = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
    for (int i = 0; i < args.Length; i++)
    {
        var arg = args[i];
        if (!arg.StartsWith("--")) continue;
        var key = arg[2..];
        string? value = null;
        if (i + 1 < args.Length && !args[i + 1].StartsWith("--"))
        {
            value = args[++i];
        }
        map[key] = value;
    }
    return map;
}

static string GetArg(Dictionary<string, string?> map, string key, string fallback) =>
    map.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value! : fallback;

static bool HasFlag(Dictionary<string, string?> map, string key) => map.ContainsKey(key) && map[key] is null;

static int SafeInt(Func<object> f)
{
    try { return Convert.ToInt32(f()); } catch { return -999; }
}

static string SafeString(Func<object> f)
{
    try { return Convert.ToString(f()) ?? string.Empty; } catch { return string.Empty; }
}

static string GetString(byte[] bytes, int start1, int length)
{
    var start = Math.Max(0, start1 - 1);
    if (start >= bytes.Length) return string.Empty;
    var len = Math.Min(length, bytes.Length - start);
    var slice = new byte[len];
    Array.Copy(bytes, start, slice, 0, len);
    return Encoding.GetEncoding(932).GetString(slice).Trim('\0', ' ', '\r', '\n');
}

static RaceSnapshot ParseRa(byte[] bytes)
{
    var year = GetString(bytes, 12, 4);
    var monthDay = GetString(bytes, 16, 4);
    var jyo = GetString(bytes, 20, 2);
    var kaiji = GetString(bytes, 22, 2);
    var nichiji = GetString(bytes, 24, 2);
    var raceNum = GetString(bytes, 26, 2);
    var hondai = GetString(bytes, 33, 60);
    var ryakusyo10 = GetString(bytes, 573, 20);
    var gradeCd = GetString(bytes, 615, 1);
    var kyori = GetString(bytes, 698, 4);
    var trackCd = GetString(bytes, 706, 2);
    var hassoTime = GetString(bytes, 874, 4);
    var raceId = $"{year}{jyo}{kaiji}{nichiji}{raceNum}";
    var date = string.Empty;
    if (DateTime.TryParseExact(year + monthDay, "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out var dt))
    {
        date = dt.ToString("yyyy-MM-dd");
    }
    var time = string.Empty;
    var sortTime = string.Empty;
    if (hassoTime.Length == 4)
    {
        time = $"{hassoTime[..2]}:{hassoTime[2..4]}";
        if (!string.IsNullOrEmpty(date)) sortTime = $"{date} {time}";
    }
    return new RaceSnapshot
    {
        RaceId = raceId,
        Date = date,
        Place = JyoToPlace(jyo),
        RaceName = !string.IsNullOrWhiteSpace(hondai) ? hondai : ryakusyo10,
        RaceNumber = int.TryParse(raceNum, out var rn) ? rn : 0,
        SortTime = sortTime,
        Time = string.IsNullOrEmpty(time) ? "TBA" : time,
        KaisaId = $"{year}{jyo}{kaiji}{nichiji}",
        Distance = int.TryParse(kyori, out var d) ? d : 0,
        Surface = TrackToSurface(trackCd),
        Grade = gradeCd,
        Entries = new List<RaceEntry>(),
    };
}

static RaceEntry ParseSe(byte[] bytes)
{
    var year = GetString(bytes, 12, 4);
    var jyo = GetString(bytes, 20, 2);
    var kaiji = GetString(bytes, 22, 2);
    var nichiji = GetString(bytes, 24, 2);
    var raceNum = GetString(bytes, 26, 2);
    return new RaceEntry
    {
        RaceId = $"{year}{jyo}{kaiji}{nichiji}{raceNum}",
        BK = GetString(bytes, 28, 1),
        PP = GetString(bytes, 29, 2),
        HorseId = GetString(bytes, 31, 10),
        Horse = GetString(bytes, 41, 36),
        Record = string.Empty,
        Sire = string.Empty,
        Dam = string.Empty,
        BMS = string.Empty,
        Odds = NormalizeOdds(GetString(bytes, 360, 4)),
        Fav = GetString(bytes, 364, 2),
        Jockey = GetString(bytes, 307, 8),
        SireId = string.Empty,
        DamId = string.Empty,
        BmsId = string.Empty,
    };
}

static string NormalizeOdds(string raw)
{
    if (string.IsNullOrWhiteSpace(raw)) return string.Empty;
    if (!int.TryParse(raw, out var v)) return raw;
    return (v / 10.0m).ToString("0.0");
}

static string JyoToPlace(string jyo) => jyo switch
{
    "01" => "Sapporo",
    "02" => "Hakodate",
    "03" => "Fukushima",
    "04" => "Niigata",
    "05" => "Tokyo",
    "06" => "Nakayama",
    "07" => "Chukyo",
    "08" => "Kyoto",
    "09" => "Hanshin",
    "10" => "Kokura",
    _ => $"JYO-{jyo}",
};

static string TrackToSurface(string trackCd)
{
    if (string.IsNullOrWhiteSpace(trackCd)) return string.Empty;
    return trackCd[0] switch
    {
        '1' => "Turf",
        '2' => "Dirt",
        '3' => "Obstacle",
        _ => trackCd,
    };
}

public sealed class Result
{
    public bool Ok { get; set; }
    public string Sid { get; set; } = string.Empty;
    public string DataSpec { get; set; } = string.Empty;
    public string FromDate { get; set; } = string.Empty;
    public string SavePath { get; set; } = string.Empty;
    public string Version { get; set; } = string.Empty;
    public int InitCode { get; set; }
    public int SetSavePathCode { get; set; }
    public int SetServiceKeyCode { get; set; }
    public int OpenCode { get; set; }
    public int CloseCode { get; set; }
    public int ReadCount { get; set; }
    public int DownloadCount { get; set; }
    public int StatusCode { get; set; }
    public int RecordsRead { get; set; }
    public string LastFileTimestamp { get; set; } = string.Empty;
    public string Error { get; set; } = string.Empty;
    public List<RaceSnapshot> Races { get; set; } = new();
}

public sealed class RaceSnapshot
{
    public string RaceId { get; set; } = string.Empty;
    public string Date { get; set; } = string.Empty;
    public string Place { get; set; } = string.Empty;
    public string RaceName { get; set; } = string.Empty;
    public int RaceNumber { get; set; }
    public string SortTime { get; set; } = string.Empty;
    public string Time { get; set; } = string.Empty;
    public string KaisaId { get; set; } = string.Empty;
    public int Distance { get; set; }
    public string Surface { get; set; } = string.Empty;
    public string Grade { get; set; } = string.Empty;
    public List<RaceEntry> Entries { get; set; } = new();
}

public sealed class RaceEntry
{
    public string RaceId { get; set; } = string.Empty;
    public string BK { get; set; } = string.Empty;
    public string PP { get; set; } = string.Empty;
    public string HorseId { get; set; } = string.Empty;
    public string Horse { get; set; } = string.Empty;
    public string Record { get; set; } = string.Empty;
    public string Sire { get; set; } = string.Empty;
    public string Dam { get; set; } = string.Empty;
    public string BMS { get; set; } = string.Empty;
    public string Odds { get; set; } = string.Empty;
    public string Fav { get; set; } = string.Empty;
    public string Jockey { get; set; } = string.Empty;
    public string SireId { get; set; } = string.Empty;
    public string DamId { get; set; } = string.Empty;
    public string BmsId { get; set; } = string.Empty;
}