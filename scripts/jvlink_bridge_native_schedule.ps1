param(
    [string]$Sid = "UMANAGER",
    [string]$ServiceKey = "",
    [string]$SavePath = "",
    [string]$DataSpec = "TOKU",
    [string]$FromDate,
    [int]$MaxRecords = 20000,
    [int]$DataOption = 1,
    [int]$MaxStatusWaitSeconds = 180,
    [switch]$SkipServiceKey
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::GetEncoding(932)

if (-not $FromDate) {
    throw "FromDate is required"
}

$source = @"
using System;
using System.Collections.Generic;
using System.Text;
using System.Web.Script.Serialization;
using System.Reflection;
using Microsoft.CSharp.RuntimeBinder;

public class NativeScheduleReader
{
    public static string Run(string sid, string savePath, string dataSpec, string fromDate, int maxRecords, int dataOption, int maxStatusWaitSeconds, bool skipServiceKey, string serviceKey)
    {
        var result = new Dictionary<string, object>();
        result["ok"] = false;
        result["sid"] = sid ?? "";
        result["dataSpec"] = dataSpec ?? "";
        result["fromDate"] = fromDate ?? "";

        try
        {
            Encoding enc = Encoding.GetEncoding(932);
            Type t = Type.GetTypeFromProgID("JVDTLab.JVLink", true);
            dynamic obj = Activator.CreateInstance(t);

            result["version"] = SafeString(() => obj.m_JVLinkVersion);
            result["initCode"] = SafeInt(() => obj.JVInit(sid));

            if (!string.IsNullOrEmpty(savePath))
            {
                result["setSavePathCode"] = SafeInt(() => obj.JVSetSavePath(savePath));
                SafeInt(() => obj.JVSetSaveFlag(1));
            }

            if (!skipServiceKey && !string.IsNullOrEmpty(serviceKey))
            {
                result["setServiceKeyCode"] = SafeInt(() => obj.JVSetServiceKey(serviceKey));
            }

            int readCount = 0;
            int downloadCount = 0;
            string lastFileTimestamp = "";
            int openCode = SafeInt(() => obj.JVOpen(dataSpec, fromDate, dataOption, ref readCount, ref downloadCount, ref lastFileTimestamp));
            result["openCode"] = openCode;
            result["readCount"] = readCount;
            result["downloadCount"] = downloadCount;
            result["lastFileTimestamp"] = lastFileTimestamp;

            int statusCode = 0;
            if (openCode >= 0 && downloadCount > 0)
            {
                DateTime deadline = DateTime.UtcNow.AddSeconds(maxStatusWaitSeconds);
                while (DateTime.UtcNow < deadline)
                {
                    statusCode = SafeInt(() => obj.JVStatus());
                    if (statusCode < 0 || statusCode >= downloadCount) break;
                    System.Threading.Thread.Sleep(200);
                }
            }
            result["statusCode"] = statusCode;

            var races = new Dictionary<string, Dictionary<string, object>>(StringComparer.OrdinalIgnoreCase);
            var horses = new List<Dictionary<string, object>>();
            var specCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            string firstBytesHex = "";
            int firstNonZeroCount = 0;
            int recordsRead = 0;
            int lastRet = 0;
            while (recordsRead < maxRecords)
            {
                int size = 0;
                string fileName = "";
                int ret = -1;
                byte[] bytes = null;

                try
                {
                    object raw = new byte[120000];
                    ret = SafeInt(() => obj.JVGets(ref raw, 120000, ref fileName));
                    if (ret > 0)
                    {
                        byte[] rawBytes = raw as byte[];
                        if (rawBytes == null && raw is Array)
                        {
                            Array arr = (Array)raw;
                            rawBytes = new byte[arr.Length];
                            for (int i = 0; i < arr.Length; i++) rawBytes[i] = Convert.ToByte(arr.GetValue(i));
                        }
                        if (rawBytes == null) rawBytes = new byte[0];
                        int used = Math.Min(ret, rawBytes.Length);
                        bytes = new byte[used];
                        Array.Copy(rawBytes, bytes, used);
                        size = used;
                    }
                }
                catch { }

                if ((bytes == null || bytes.Length <= 0) && ret > 0)
                {
                    // Final fallback for environments where JVGets may fail on specific records.
                    // We keep this as fallback-only to avoid cursor advancement issues from mixed reads.
                    try
                    {
                        string buff = "";
                        ret = SafeInt(() => obj.JVRead(ref buff, ref size, ref fileName));
                        if (ret > 0 && !string.IsNullOrEmpty(buff) && size > 0)
                        {
                            bytes = enc.GetBytes(buff);
                        }
                    }
                    catch { }
                }

                lastRet = ret;
                if (ret == -1)
                {
                    continue;
                }
                if (ret == 0)
                {
                    break;
                }
                if (ret < -1 || bytes == null || bytes.Length <= 0)
                {
                    break;
                }
                recordsRead++;
                if (recordsRead == 1)
                {
                    int nz = 0;
                    int take = Math.Min(24, bytes.Length);
                    var hex = new StringBuilder();
                    for (int i = 0; i < bytes.Length; i++) if (bytes[i] != 0) nz++;
                    for (int i = 0; i < take; i++)
                    {
                        if (i > 0) hex.Append(" ");
                        hex.Append(bytes[i].ToString("X2"));
                    }
                    firstBytesHex = hex.ToString();
                    firstNonZeroCount = nz;
                }
                if (bytes.Length < 30) continue;
                string spec = GetString(bytes, 1, 2);
                if (!specCounts.ContainsKey(spec)) specCounts[spec] = 0;
                specCounts[spec] = specCounts[spec] + 1;
                if (spec == "RA")
                {
                    var race = ParseRa(bytes);
                    if (race.ContainsKey("raceId"))
                    {
                        string raceId = (string)race["raceId"];
                        if (races.ContainsKey(raceId))
                        {
                            // Merge RA metadata into existing shell (e.g. TK/JG shell created earlier).
                            // Preserve the existing entries list so SE records added before RA are not lost.
                            var existing = races[raceId];
                            foreach (var key in race.Keys)
                            {
                                if (key == "entries") continue;
                                existing[key] = race[key];
                            }
                        }
                        else
                        {
                            race["entries"] = new List<Dictionary<string, object>>();
                            races[raceId] = race;
                        }
                    }
                }
                else if (spec == "SE")
                {
                    var entry = ParseSe(bytes);
                    if (entry.ContainsKey("raceId"))
                    {
                        string raceId = (string)entry["raceId"];
                        if (!races.ContainsKey(raceId))
                        {
                            var raceShell = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                            raceShell.Add("raceId", raceId);
                            raceShell.Add("date", "");
                            raceShell.Add("place", "");
                            raceShell.Add("raceName", "");
                            raceShell.Add("raceNumber", 0);
                            raceShell.Add("sortTime", "");
                            raceShell.Add("time", "TBA");
                            raceShell.Add("kaisaiId", "");
                            raceShell.Add("distance", 0);
                            raceShell.Add("surface", "");
                            raceShell.Add("grade", "");
                            raceShell.Add("entries", new List<Dictionary<string, object>>());
                            races[raceId] = raceShell;
                        }
                        ((List<Dictionary<string, object>>)races[raceId]["entries"]).Add(entry);
                    }
                }
                else if (spec == "TK")
                {
                    // JV_TK_TOKUUMA: special/graded race registration record.
                    // Published weeks before the race; useful as an early race shell.
                    var tkRace = ParseTk(bytes);
                    if (tkRace.ContainsKey("raceId"))
                    {
                        string raceId = (string)tkRace["raceId"];
                        if (!races.ContainsKey(raceId))
                        {
                            tkRace["entries"] = new List<Dictionary<string, object>>();
                            races[raceId] = tkRace;
                        }
                    }
                }
                else if (spec == "JG")
                {
                    // JV_JG_JOGAIBA: horse exclusion/scratch record.
                    // References races via RACE_ID; creates a minimal race shell if none exists.
                    var jgRace = ParseJg(bytes);
                    if (jgRace.ContainsKey("raceId"))
                    {
                        string raceId = (string)jgRace["raceId"];
                        if (!races.ContainsKey(raceId))
                        {
                            jgRace["entries"] = new List<Dictionary<string, object>>();
                            races[raceId] = jgRace;
                        }
                    }
                }
                else if (spec == "UM")
                {
                    // JV_UM_UMA: horse pedigree record.
                    var horse = ParseUm(bytes);
                    horses.Add(horse);
                }
            }

            result["recordsRead"] = recordsRead;
            result["lastReadRet"] = lastRet;
            result["specCounts"] = specCounts;
            result["firstBytesHex"] = firstBytesHex;
            result["firstNonZeroCount"] = firstNonZeroCount;
            result["races"] = new List<Dictionary<string, object>>(races.Values);
            result["horses"] = horses;
            result["closeCode"] = SafeInt(() => obj.JVClose());
            result["ok"] = true;
        }
        catch (Exception ex)
        {
            result["error"] = ex.ToString();
        }

        var serializer = new JavaScriptSerializer();
        serializer.MaxJsonLength = int.MaxValue;
        serializer.RecursionLimit = 512;
        return serializer.Serialize(result);
    }

    static int SafeInt(Func<object> f) { try { return Convert.ToInt32(f()); } catch { return -999; } }
    static string SafeString(Func<object> f) { try { return Convert.ToString(f()) ?? ""; } catch { return ""; } }

    static string GetString(byte[] bytes, int start1, int length)
    {
        int start = Math.Max(0, start1 - 1);
        if (start >= bytes.Length) return "";
        int len = Math.Min(length, bytes.Length - start);
        byte[] slice = new byte[len];
        Array.Copy(bytes, start, slice, 0, len);
        return Encoding.GetEncoding(932).GetString(slice).Trim('\0', ' ', '\r', '\n');
    }

    // Returns raw bytes as lowercase hex — used for Japanese text fields to bypass
    // locale-dependent encoding when passing data through stdout to Python.
    // Python decodes: bytes.fromhex(hex).rstrip(b'\x00').decode('cp932').strip()
    static string GetHex(byte[] bytes, int start1, int length)
    {
        int start = Math.Max(0, start1 - 1);
        if (start >= bytes.Length) return "";
        int len = Math.Min(length, bytes.Length - start);
        var sb = new StringBuilder(len * 2);
        for (int i = start; i < start + len; i++)
            sb.Append(bytes[i].ToString("x2"));
        return sb.ToString();
    }

    static Dictionary<string, object> ParseRa(byte[] bytes)
    {
        string year = GetString(bytes, 12, 4);
        string monthDay = GetString(bytes, 16, 4);
        string jyo = GetString(bytes, 20, 2);
        string kaiji = GetString(bytes, 22, 2);
        string nichiji = GetString(bytes, 24, 2);
        string raceNum = GetString(bytes, 26, 2);
        string hondai = GetHex(bytes, 33, 60);
        string ryakusyo10 = GetHex(bytes, 573, 20);
        string gradeCd = GetString(bytes, 615, 1);
        string kyori = GetString(bytes, 698, 4);
        string trackCd = GetString(bytes, 706, 2);
        string hassoTime = GetString(bytes, 874, 4);
        string torokuTosu = GetString(bytes, 882, 2);
        string syussoTosu = GetString(bytes, 884, 2);
        string raceId = year + jyo + kaiji + nichiji + raceNum;
        string date = "";
        DateTime dt;
        if (DateTime.TryParseExact(year + monthDay, "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out dt))
        {
            date = dt.ToString("yyyy-MM-dd");
        }
        string time = hassoTime.Length == 4 ? hassoTime.Substring(0, 2) + ":" + hassoTime.Substring(2, 2) : "TBA";
        string sortTime = (!string.IsNullOrEmpty(date) && hassoTime.Length == 4) ? date + " " + time : "";
        int raceNumber = 0;
        int.TryParse(raceNum, out raceNumber);
        int distance = 0;
        int.TryParse(kyori, out distance);
        int registeredCount = 0;
        int.TryParse(torokuTosu, out registeredCount);
        int starterCount = 0;
        int.TryParse(syussoTosu, out starterCount);

        var race = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        race.Add("raceId", raceId);
        race.Add("date", date);
        race.Add("place", JyoToPlace(jyo));
        race.Add("raceName", !string.IsNullOrEmpty(hondai) ? hondai : ryakusyo10);
        race.Add("raceNumber", raceNumber);
        race.Add("sortTime", sortTime);
        race.Add("time", time);
        race.Add("kaisaiId", year + jyo + kaiji + nichiji);
        race.Add("distance", distance);
        race.Add("surface", TrackToSurface(trackCd));
        race.Add("grade", gradeCd);
        race.Add("registeredCount", registeredCount);
        race.Add("starterCount", starterCount);
        return race;
    }

    static Dictionary<string, object> ParseSe(byte[] bytes)
    {
        string year = GetString(bytes, 12, 4);
        string jyo = GetString(bytes, 20, 2);
        string kaiji = GetString(bytes, 22, 2);
        string nichiji = GetString(bytes, 24, 2);
        string raceNum = GetString(bytes, 26, 2);
        string wakuban = GetString(bytes, 28, 1);
        string umaban = GetString(bytes, 29, 2);
        string oddsRaw = GetString(bytes, 360, 4);
        string favRaw = GetString(bytes, 364, 2);
        // JV-Data 4.9 spec: KakuteiJyuni at position 335 (1-indexed), length 2.
        // GetString is 1-indexed (subtracts 1 internally), so pass 335 directly.
        string kakuteiJyuni = bytes.Length >= 336 ? GetString(bytes, 335, 2) : "";
        int oddsInt;
        string odds = Int32.TryParse(oddsRaw, out oddsInt) && oddsInt > 0 ? (oddsInt / 10.0m).ToString("0.0") : "";

        // Normalize zero-placeholder fields to blanks.
        int tmp;
        if (Int32.TryParse(wakuban, out tmp) && tmp <= 0) wakuban = "";
        if (Int32.TryParse(umaban, out tmp) && tmp <= 0) umaban = "";
        if (Int32.TryParse(favRaw, out tmp) && tmp <= 0) favRaw = "";
        if (Int32.TryParse(kakuteiJyuni, out tmp) && tmp <= 0) kakuteiJyuni = "";

        var entry = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        entry.Add("raceId", year + jyo + kaiji + nichiji + raceNum);
        entry.Add("BK", wakuban);
        entry.Add("PP", umaban);
        entry.Add("Horse_ID", GetString(bytes, 31, 10));
        entry.Add("Horse", GetHex(bytes, 41, 36));
        entry.Add("Record", "");
        entry.Add("Finish", kakuteiJyuni);
        entry.Add("Sire", "");
        entry.Add("Dam", "");
        entry.Add("BMS", "");
        entry.Add("Odds", odds);
        entry.Add("Fav", favRaw);
        entry.Add("Jockey", GetHex(bytes, 307, 8));
        entry.Add("Sire_ID", "");
        entry.Add("Dam_ID", "");
        entry.Add("BMS_ID", "");
        return entry;
    }

    // JV_TK_TOKUUMA — special/graded race registration record.
    // RACE_ID at bytes 12-27.  RACE_INFO.Hondai at bytes 33-92 (same offsets as RA).
    // TK-specific: GradeCD=615, Kyori=637, TrackCD=641, TorokuTosu=653.
    static Dictionary<string, object> ParseTk(byte[] bytes)
    {
        string year      = GetString(bytes, 12, 4);
        string monthDay  = GetString(bytes, 16, 4);
        string jyo       = GetString(bytes, 20, 2);
        string kaiji     = GetString(bytes, 22, 2);
        string nichiji   = GetString(bytes, 24, 2);
        string raceNum   = GetString(bytes, 26, 2);
        string hondai    = GetHex(bytes, 33, 60);
        string ryakusyo  = GetHex(bytes, 573, 20);
        string gradeCd   = bytes.Length >= 615 ? GetString(bytes, 615, 1) : "";
        string kyori     = bytes.Length >= 640 ? GetString(bytes, 637, 4) : "";
        string trackCd   = bytes.Length >= 642 ? GetString(bytes, 641, 2) : "";
        string raceId    = year + jyo + kaiji + nichiji + raceNum;
        string date      = "";
        DateTime dt;
        if (DateTime.TryParseExact(year + monthDay, "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out dt))
            date = dt.ToString("yyyy-MM-dd");
        int raceNumber = 0;
        int.TryParse(raceNum, out raceNumber);
        int distance = 0;
        int.TryParse(kyori, out distance);
        var race = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        race.Add("raceId",     raceId);
        race.Add("date",       date);
        race.Add("place",      JyoToPlace(jyo));
        race.Add("raceName",   !string.IsNullOrEmpty(hondai) ? hondai : ryakusyo);
        race.Add("raceNumber", raceNumber);
        race.Add("sortTime",   date);
        race.Add("time",       "TBA");
        race.Add("kaisaiId",   year + jyo + kaiji + nichiji);
        race.Add("distance",   distance);
        race.Add("surface",    TrackToSurface(trackCd));
        race.Add("grade",      gradeCd);
        race.Add("source",     "TK");
        return race;
    }

    // JV_JG_JOGAIBA — horse exclusion/scratch record.  80-byte record.
    // RACE_ID at bytes 12-27.  Used only for race discovery (scratch horses are not added as entries).
    static Dictionary<string, object> ParseJg(byte[] bytes)
    {
        string year     = GetString(bytes, 12, 4);
        string monthDay = GetString(bytes, 16, 4);
        string jyo      = GetString(bytes, 20, 2);
        string kaiji    = GetString(bytes, 22, 2);
        string nichiji  = GetString(bytes, 24, 2);
        string raceNum  = GetString(bytes, 26, 2);
        string raceId   = year + jyo + kaiji + nichiji + raceNum;
        string date     = "";
        DateTime dt;
        if (DateTime.TryParseExact(year + monthDay, "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out dt))
            date = dt.ToString("yyyy-MM-dd");
        int raceNumber = 0;
        int.TryParse(raceNum, out raceNumber);
        var race = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        race.Add("raceId",     raceId);
        race.Add("date",       date);
        race.Add("place",      JyoToPlace(jyo));
        race.Add("raceName",   "");
        race.Add("raceNumber", raceNumber);
        race.Add("sortTime",   date);
        race.Add("time",       "TBA");
        race.Add("kaisaiId",   year + jyo + kaiji + nichiji);
        race.Add("distance",   0);
        race.Add("surface",    "");
        race.Add("grade",      "");
        race.Add("source",     "JG");
        return race;
    }

    static Dictionary<string, object> ParseUm(byte[] bytes)
    {
        string kettoNum = GetString(bytes, 12, 10);
        string bamei = GetHex(bytes, 47, 36);
        string sireId = "";
        string sireJp = "";
        string damId = "";
        string damJp = "";
        string bmsId = "";
        string bmsJp = "";

        // JV-Data 4.9 spec: Ketto3Info pedigree array starts at byte 185 (1-indexed).
        // Each slot is 46 bytes: HansyokuNum(10) + Bamei(36)
        // Slot order (0-indexed): sire=0, dam=1, sire-sire=2, sire-dam=3, BMS=dam-sire=4
        if (bytes.Length >= 415)  // need through slot 4: (185-1) + 4*46 + 46 = 414, so >= 415
        {
            sireId = GetString(bytes, 185, 10);
            sireJp = GetHex(bytes, 195, 36);
            damId  = GetString(bytes, 231, 10);  // 185 + 46
            damJp  = GetHex(bytes, 241, 36);
            bmsId  = GetString(bytes, 369, 10);  // 185 + 4*46
            bmsJp  = GetHex(bytes, 379, 36);
        }
        
        var horse = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        horse.Add("KettoNum", kettoNum);
        horse.Add("UmaName", bamei);
        horse.Add("Sire_ID", sireId);
        horse.Add("Sire_JP", sireJp);
        horse.Add("Dam_ID", damId);
        horse.Add("Dam_JP", damJp);
        horse.Add("BMS_ID", bmsId);
        horse.Add("BMS_JP", bmsJp);
        return horse;
    }

    static string JyoToPlace(string jyo)
    {
        switch (jyo)
        {
            case "01": return "Sapporo";
            case "02": return "Hakodate";
            case "03": return "Fukushima";
            case "04": return "Niigata";
            case "05": return "Tokyo";
            case "06": return "Nakayama";
            case "07": return "Chukyo";
            case "08": return "Kyoto";
            case "09": return "Hanshin";
            case "10": return "Kokura";
            default: return "JYO-" + jyo;
        }
    }

    static string TrackToSurface(string trackCd)
    {
        if (string.IsNullOrEmpty(trackCd)) return "";
        switch (trackCd[0])
        {
            case '1': return "Turf";
            case '2': return "Dirt";
            case '3': return "Obstacle";
            default: return trackCd;
        }
    }
}
"@

Add-Type -TypeDefinition $source -Language CSharp -ReferencedAssemblies @(
    "System.dll",
    "System.Web.Extensions.dll",
    "Microsoft.CSharp.dll"
)

$json = [NativeScheduleReader]::Run(
    [string]$Sid,
    [string]$SavePath,
    [string]$DataSpec,
    [string]$FromDate,
    [int]$MaxRecords,
    [int]$DataOption,
    [int]$MaxStatusWaitSeconds,
    [bool]$SkipServiceKey,
    [string]$ServiceKey
)

Write-Output $json