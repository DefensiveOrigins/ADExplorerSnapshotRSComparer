using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Formats.Tar;

// ------------------------------------------------------------
// ADExplorer .tar.gz Snapshot Diff -> HTML (C# / .NET 8)
// Usage:
//   AdExpDiffTar --oldtgz old.tar.gz --newtgz new.tar.gz --output report.html
// ------------------------------------------------------------

return App.Run(args);

static class App
{
    public static int Run(string[] args)
    {
        try
        {
            var opts = Options.Parse(args);
            if (!opts.IsValid(out var why))
            {
                Console.Error.WriteLine(why);
                Options.PrintHelp();
                return 2;
            }

            Console.WriteLine("Reading archives...");
            var oldMap = LoadArchive(opts.OldTgz!, opts, out var oldStats);
            var newMap = LoadArchive(opts.NewTgz!, opts, out var newStats);

            Console.WriteLine($"[INFO] {Path.GetFileName(opts.OldTgz!)} -> files: {oldStats.files}, objects: {oldStats.objects}");
            Console.WriteLine($"[INFO] {Path.GetFileName(opts.NewTgz!)} -> files: {newStats.files}, objects: {newStats.objects}");

            Console.WriteLine("Diffing...");
            var diff = Diff(oldMap, newMap);

            Console.WriteLine("Rendering HTML...");
            var html = RenderHtml(opts, diff);
            File.WriteAllText(opts.Output!, html, new UTF8Encoding(false));

            Console.WriteLine($"Done. Wrote -> {Path.GetFullPath(opts.Output!)}");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("ERROR: " + ex.Message);
            return 1;
        }
    }

    // ----------------------- Options ------------------------
    private sealed class Options
    {
        public string? OldTgz { get; set; }
        public string? NewTgz { get; set; }
        public string? Output { get; set; }

        // Ignore volatile/noisy attributes by default
        public HashSet<string> Ignore { get; } = new(StringComparer.OrdinalIgnoreCase)
        {
            "uSNChanged","uSNCreated","whenChanged","whenCreated","modifyTimestamp","createTimestamp",
            "lastLogon","lastLogonTimestamp","lastLogoff","pwdLastSet","badPwdCount","badPasswordTime",
            "lockoutTime","dSCorePropagationData","msDS-ReplAttributeMetaData",
            "objectGUID","objectSid","msDS-KeyVersionNumber"
        };

        // Split multi-values on these separators (default: ;  |  newline)
        public string[] MultiValueSeparators { get; } = new[] { ";", "|", "\n" };

        public static Options Parse(string[] args)
        {
            var o = new Options();
            for (int i = 0; i < args.Length; i++)
            {
                var a = args[i];
                string Next() => (i + 1 < args.Length) ? args[++i] : throw new ArgumentException($"Missing value for {a}");
                switch (a.ToLowerInvariant())
                {
                    case "--oldtgz": o.OldTgz = Next(); break;
                    case "--newtgz": o.NewTgz = Next(); break;
                    case "--output": o.Output = Next(); break;
                    case "--help":
                    case "-h":
                    case "/?":
                        PrintHelp();
                        Environment.Exit(0);
                        break;
                }
            }
            return o;
        }

        public bool IsValid(out string why)
        {
            if (string.IsNullOrWhiteSpace(OldTgz) || !File.Exists(OldTgz))
            { why = "Provide --oldtgz <path-to-old.tar.gz>"; return false; }
            if (string.IsNullOrWhiteSpace(NewTgz) || !File.Exists(NewTgz))
            { why = "Provide --newtgz <path-to-new.tar.gz>"; return false; }
            if (string.IsNullOrWhiteSpace(Output))
            { why = "Provide --output <path-to-report.html>"; return false; }
            why = "";
            return true;
        }

        public static void PrintHelp()
        {
            Console.WriteLine(@"
ADExplorer Snapshot Diff from .tar.gz (C# / .NET 8)

Required:
  --oldtgz <file>     Path to older snapshot .tar.gz (from Rust converter)
  --newtgz <file>     Path to newer snapshot .tar.gz
  --output <file>     Output HTML file path

Example:
  AdExpDiffTar --oldtgz C:\snap\old.tar.gz --newtgz C:\snap\new.tar.gz --output C:\out\ad_diff.html
");
        }
    }

    // ----------------- Archive -> Object Map ----------------
    private static Dictionary<string, ObjRec> LoadArchive(
        string tgzPath, Options o, out (int files, int objects) stats)
    {
        using var fs = File.OpenRead(tgzPath);
        using var gz = new GZipStream(fs, CompressionMode.Decompress, leaveOpen: false);
        using var tar = new TarReader(gz);

        var map = new Dictionary<string, ObjRec>(StringComparer.OrdinalIgnoreCase);
        var sepRegex = BuildSeparatorRegex(o.MultiValueSeparators);
        var ignore = o.Ignore;

        int filesSeen = 0, objectsSeen = 0;

        TarEntry? entry;
        while ((entry = tar.GetNextEntry()) is not null)
        {
            if (entry.EntryType is not TarEntryType.RegularFile and not TarEntryType.V7RegularFile)
                continue;

            var name = entry.Name ?? "";
            var lower = name.ToLowerInvariant();

            if (!lower.EndsWith(".json") && !lower.EndsWith(".json.gz"))
                continue;

            using var ms = new MemoryStream();
            entry.DataStream!.CopyTo(ms);
            var payload = ms.ToArray();

            if (lower.EndsWith(".json.gz"))
            {
                using var innerMs = new MemoryStream(payload);
                using var innerGz = new GZipStream(innerMs, CompressionMode.Decompress);
                using var outMs = new MemoryStream();
                innerGz.CopyTo(outMs);
                payload = outMs.ToArray();
            }

            var json = Encoding.UTF8.GetString(payload);
            var fileStem = Path.GetFileNameWithoutExtension(
                               Path.GetFileNameWithoutExtension(name)); // strip .json.gz

            filesSeen++;
            objectsSeen += LoadJsonFile(json, fileStem, map, ignore, sepRegex);
        }

        stats = (filesSeen, objectsSeen);
        return map;
    }

    private static int LoadJsonFile(string json, string fileStem,
        Dictionary<string, ObjRec> map, HashSet<string> ignore, Regex sep)
    {
        using var doc = JsonDocument.Parse(json);
        int added = 0;

        // Try common container shapes
        if (doc.RootElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var el in doc.RootElement.EnumerateArray())
                if (AddFromElement(el, fileStem, map, ignore, sep)) added++;
            return added;
        }

        if (doc.RootElement.ValueKind == JsonValueKind.Object)
        {
            // data / nodes / objects / items / rows / entries
            foreach (var key in new[] { "data", "nodes", "objects", "items", "rows", "entries" })
            {
                if (doc.RootElement.TryGetProperty(key, out var arr) && arr.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in arr.EnumerateArray())
                        if (AddFromElement(el, fileStem, map, ignore, sep)) added++;
                    return added;
                }
            }

            // Fallback single object
            if (AddFromElement(doc.RootElement, fileStem, map, ignore, sep)) added++;
        }

        return added;
    }

    private static bool AddFromElement(JsonElement el, string fileStem,
        Dictionary<string, ObjRec> map, HashSet<string> ignore, Regex sep)
    {
        // Prefer Properties if present (BloodHound-like)
        JsonElement attrSource = el;
        if (el.TryGetProperty("Properties", out var props) && props.ValueKind == JsonValueKind.Object)
            attrSource = props;

        // Key detection from BOTH root and attrSource (order matters)
        string? key =
            TryGetString(el, "DistinguishedName") ?? TryGetString(attrSource, "DistinguishedName") ??
            TryGetString(el, "distinguishedname") ?? TryGetString(attrSource, "distinguishedname") ??
            TryGetString(el, "dn") ?? TryGetString(attrSource, "dn") ??
            TryGetString(el, "ObjectIdentifier") ?? TryGetString(attrSource, "ObjectIdentifier") ??
            TryGetString(el, "objectid") ?? TryGetString(attrSource, "objectid") ??
            TryGetString(el, "Guid") ?? TryGetString(attrSource, "Guid") ??
            TryGetString(el, "guid") ?? TryGetString(attrSource, "guid") ??
            TryGetString(el, "Name") ?? TryGetString(attrSource, "Name") ??
            TryGetString(el, "name") ?? TryGetString(attrSource, "name") ??
            TryGetString(el, "id") ?? TryGetString(attrSource, "id");

        if (string.IsNullOrWhiteSpace(key))
            return false;

        // If key is too generic, namespace by file to avoid collisions
        if (!key.Contains("=") && !key.Contains("{") && !key.Contains("@") && !key.Contains("-") && !key.Contains(","))
            key = $"{fileStem}:{key}";

        var attrs = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

        // Collect from attrSource
        foreach (var p in attrSource.EnumerateObject())
        {
            if (ignore.Contains(p.Name)) continue;

            // Skip common identity fields to reduce noise
            if (p.NameEquals("DistinguishedName") || p.NameEquals("distinguishedname") || p.NameEquals("dn")) continue;
            if (p.NameEquals("ObjectIdentifier") || p.NameEquals("objectid")) continue;
            if (p.NameEquals("Guid") || p.NameEquals("guid")) continue;
            if (p.NameEquals("name") || p.NameEquals("Name") || p.NameEquals("id")) continue;
            if (p.NameEquals("objectClass")) continue;

            var values = NormalizeValues(p.Value, sep);
            if (values.Length > 0) attrs[p.Name] = values;
        }

        // Also pull simple fields from root (next to Properties) if present
        if (el.ValueKind == JsonValueKind.Object && !ReferenceEquals(el, attrSource))
        {
            foreach (var p in el.EnumerateObject())
            {
                if (p.NameEquals("Properties")) continue;
                if (ignore.Contains(p.Name)) continue;
                if (p.Value.ValueKind is JsonValueKind.String or JsonValueKind.Array or JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False)
                {
                    var values = NormalizeValues(p.Value, sep);
                    if (values.Length > 0)
                    {
                        if (attrs.TryGetValue(p.Name, out var existing))
                            attrs[p.Name] = existing.Union(values, StringComparer.OrdinalIgnoreCase).OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray();
                        else
                            attrs[p.Name] = values;
                    }
                }
            }
        }

        var fp = string.Join("|", attrs.OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase)
                                       .Select(kv => $"{kv.Key}={string.Join("||", kv.Value)}"));

        map[key] = new ObjRec(key, attrs, fp);
        return true;
    }

    private static string? TryGetString(JsonElement el, string name)
        => el.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    private static Regex BuildSeparatorRegex(string[] seps)
        => new(string.Join("|", seps.Select(Regex.Escape)), RegexOptions.Compiled);

    private static string[] NormalizeValues(JsonElement val, Regex sep)
    {
        List<string> acc = new();

        switch (val.ValueKind)
        {
            case JsonValueKind.Array:
                foreach (var v in val.EnumerateArray())
                    acc.Add(v.ValueKind == JsonValueKind.String ? (v.GetString() ?? "") : v.ToString());
                break;

            case JsonValueKind.String:
                var s = (val.GetString() ?? "").Replace("\r\n", "\n").Replace("\r", "\n");
                acc.AddRange(sep.Split(s));
                break;

            case JsonValueKind.Number:
            case JsonValueKind.True:
            case JsonValueKind.False:
                acc.Add(val.ToString());
                break;

            default:
                // objects/null -> ignore
                break;
        }

        acc = acc.Select(x => x.Trim())
                 .Where(x => !string.IsNullOrEmpty(x))
                 .Distinct(StringComparer.OrdinalIgnoreCase)
                 .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                 .ToList();

        return acc.ToArray();
    }

    // ------------------------ Diffing -----------------------
    private sealed record ObjRec(string Key, Dictionary<string, string[]> Attrs, string Fingerprint);

    private sealed class DiffResult
    {
        public required int OldCount { get; init; }
        public required int NewCount { get; init; }
        public required List<string> Added { get; init; }
        public required List<string> Deleted { get; init; }
        public required List<ModifiedObj> Modified { get; init; }
    }

    private sealed class ModifiedObj
    {
        public required string Dn { get; init; }
        public required List<AttrChange> Changes { get; init; }
    }

    private sealed class AttrChange
    {
        public required string Attribute { get; init; }
        public required string[] OldValues { get; init; }
        public required string[] NewValues { get; init; }
        public required string[] Added { get; init; }
        public required string[] Removed { get; init; }
    }

    private static DiffResult Diff(Dictionary<string, ObjRec> oldMap, Dictionary<string, ObjRec> newMap)
    {
        var cmp = StringComparer.OrdinalIgnoreCase;
        var oldKeys = oldMap.Keys.ToHashSet(cmp);
        var newKeys = newMap.Keys.ToHashSet(cmp);

        var added = newKeys.Except(oldKeys, cmp).OrderBy(x => x, cmp).ToList();
        var deleted = oldKeys.Except(newKeys, cmp).OrderBy(x => x, cmp).ToList();
        var common = newKeys.Intersect(oldKeys, cmp);

        var modified = new List<ModifiedObj>();
        foreach (var k in common)
        {
            var o = oldMap[k];
            var n = newMap[k];
            if (!string.Equals(o.Fingerprint, n.Fingerprint, StringComparison.Ordinal))
            {
                var names = o.Attrs.Keys.Union(n.Attrs.Keys, cmp).OrderBy(x => x, cmp);
                var chgs = new List<AttrChange>();
                foreach (var a in names)
                {
                    var ov = o.Attrs.TryGetValue(a, out var ovv) ? ovv : Array.Empty<string>();
                    var nv = n.Attrs.TryGetValue(a, out var nvv) ? nvv : Array.Empty<string>();
                    if (!Enumerable.SequenceEqual(ov, nv, StringComparer.OrdinalIgnoreCase))
                    {
                        chgs.Add(new AttrChange
                        {
                            Attribute = a,
                            OldValues = ov,
                            NewValues = nv,
                            Added = nv.Except(ov, StringComparer.OrdinalIgnoreCase).ToArray(),
                            Removed = ov.Except(nv, StringComparer.OrdinalIgnoreCase).ToArray()
                        });
                    }
                }
                modified.Add(new ModifiedObj { Dn = k, Changes = chgs });
            }
        }

        return new DiffResult
        {
            OldCount = oldMap.Count,
            NewCount = newMap.Count,
            Added = added,
            Deleted = deleted,
            Modified = modified.OrderBy(m => m.Dn, cmp).ToList()
        };
    }

    // ------------------------ HTML --------------------------
    private static string HtmlEncode(string? s) =>
        s is null ? "" :
        s.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("\"", "&quot;").Replace("'", "&#39;");

    private static string AsHtmlList(IEnumerable<string> items)
    {
        var arr = items?.ToArray() ?? Array.Empty<string>();
        if (arr.Length == 0) return "<em>none</em>";
        var sb = new StringBuilder("<ul>");
        foreach (var it in arr) sb.Append("<li><code>").Append(HtmlEncode(it)).Append("</code></li>");
        sb.Append("</ul>");
        return sb.ToString();
    }

    private static string RenderHtml(Options o, DiffResult d)
    {
        var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        var style = @"
<style>
 body { font-family: Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
 h1 { margin-top: 0; }
 .summary { display: grid; grid-template-columns: repeat(5, minmax(120px,1fr)); gap: 12px; margin-bottom: 20px; }
 .card { border: 1px solid #ddd; border-radius: 10px; padding: 12px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }
 .count { font-size: 22px; font-weight: 700; }
 details { margin: 8px 0 14px 0; }
 summary { cursor: pointer; font-weight: 600; }
 code { background: #f6f8fa; padding: 1px 4px; border-radius: 4px; }
 table { border-collapse: collapse; width: 100%; margin-top: 8px; }
 th, td { border: 1px solid #eee; padding: 6px 8px; vertical-align: top; }
 th { background: #fafafa; text-align: left; }
 .added { color: #0a7f2e; }
 .removed { color: #b00020; }
 .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, 'Liberation Mono', monospace; }
 .section { margin-top: 24px; }
 .small { color: #555; font-size: 12px; }
</style>";

        var addedHtml = d.Added.Count == 0
            ? "<p><em>No new objects.</em></p>"
            : $"<ul>{string.Join("", d.Added.Select(k => $"<li class='mono'><code>{HtmlEncode(k)}</code></li>"))}</ul>";

        var deletedHtml = d.Deleted.Count == 0
            ? "<p><em>No deleted objects.</em></p>"
            : $"<ul>{string.Join("", d.Deleted.Select(k => $"<li class='mono'><code>{HtmlEncode(k)}</code></li>"))}</ul>";

        var modifiedHtml = new StringBuilder();
        if (d.Modified.Count == 0)
        {
            modifiedHtml.Append("<p><em>No modified objects.</em></p>");
        }
        else
        {
            foreach (var m in d.Modified)
            {
                var rows = new StringBuilder();
                foreach (var ch in m.Changes)
                {
                    var adds = ch.Added.Length > 0
                        ? "<div class='added small'>+ Added: " + string.Join(", ", ch.Added.Select(v => $"<code>{HtmlEncode(v)}</code>")) + "</div>"
                        : "";
                    var rems = ch.Removed.Length > 0
                        ? "<div class='removed small'>− Removed: " + string.Join(", ", ch.Removed.Select(v => $"<code>{HtmlEncode(v)}</code>")) + "</div>"
                        : "";

                    rows.Append($@"
<tr>
  <td class='mono'><code>{HtmlEncode(ch.Attribute)}</code></td>
  <td>{AsHtmlList(ch.OldValues)}</td>
  <td>{AsHtmlList(ch.NewValues)} {adds} {rems}</td>
</tr>");
                }

                modifiedHtml.Append($@"
<details>
  <summary><span class='mono'><code>{HtmlEncode(m.Dn)}</code></span> &nbsp; <span class='small'>({m.Changes.Count} changed attribute(s))</span></summary>
  <table>
    <thead><tr><th>Attribute</th><th>Old</th><th>New / Delta</th></tr></thead>
    <tbody>
      {rows}
    </tbody>
  </table>
</details>");
            }
        }

        var notes = $@"
<ul>
  <li>Compared objects extracted from <code>.tar.gz</code> archives produced by the Rust converter.</li>
  <li>Multi-valued attributes are split on <code>;</code>, <code>|</code>, and <code>newline</code> and compared case-insensitively.</li>
  <li>Ignored attributes: <code>uSNChanged</code>, <code>whenChanged</code>, <code>lastLogon</code>, etc. (hard-coded defaults).</li>
</ul>";

        return $@"<!doctype html>
<html lang=""en"">
<head>
<meta charset=""utf-8"">
<title>ADExplorer Snapshot Diff</title>
{style}
</head>
<body>
  <h1>ADExplorer Snapshot Diff</h1>
  <div class=""small"">Generated: {HtmlEncode(ts)}</div>
  <div class=""small"">Old archive: <span class=""mono""><code>{HtmlEncode(Path.GetFullPath(o.OldTgz!))}</code></span></div>
  <div class=""small"">New archive: <span class=""mono""><code>{HtmlEncode(Path.GetFullPath(o.NewTgz!))}</code></span></div>

  <div class=""summary"">
    <div class=""card""><div>Old objects</div><div class=""count"">{d.OldCount}</div></div>
    <div class=""card""><div>New objects</div><div class=""count"">{d.NewCount}</div></div>
    <div class=""card""><div>Added</div><div class=""count"">{d.Added.Count}</div></div>
    <div class=""card""><div>Deleted</div><div class=""count"">{d.Deleted.Count}</div></div>
    <div class=""card""><div>Modified</div><div class=""count"">{d.Modified.Count}</div></div>
  </div>

  <div class=""section"">
    <h2>Added Objects</h2>
    {addedHtml}
  </div>

  <div class=""section"">
    <h2>Deleted Objects</h2>
    {deletedHtml}
  </div>

  <div class=""section"">
    <h2>Modified Objects</h2>
    {modifiedHtml}
  </div>

  <hr />
  <div class=""small"">
    <strong>Notes:</strong>
    {notes}
  </div>
</body>
</html>";
    }
}
