// ============================================================
// FILE: TrayApp.cs  (UMAnager.Tray)
// LAYER: Supervisor (WinForms tray app)
// PURPOSE: System-tray context menu to start/stop/restart the Nexus + Sidecar EXEs,
//          monitor their PIDs (.service-pids.json), and tail their logs.
// KEY DEPENDENCIES: System.Diagnostics.Process, WinForms NotifyIcon.
// CAUTION: EXE paths are hard-coded to bin/Release output dirs (net8.0 / net8.0-windows
//          win-x86). Rediscovers already-running services on launch.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Diagnostics;
using System.Drawing.Drawing2D;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace UMAnager.Tray;

internal sealed class TrayApp : ApplicationContext
{
    private readonly NotifyIcon _icon;
    private readonly System.Windows.Forms.Timer _timer;
    private readonly ToolStripMenuItem _startItem;
    private readonly ToolStripMenuItem _stopItem;
    private readonly ToolStripMenuItem _restartItem;

    private readonly string _rootDir;
    private readonly string _sidecarExe;
    private readonly string _nexusExe;
    private readonly string _pidFile;
    private readonly string _sidecarLog;
    private readonly string _nexusLog;

    private Process? _sidecarProc;
    private Process? _nexusProc;
    private IntPtr _currentHIcon = IntPtr.Zero;
    private bool _autoStartPending;

    [DllImport("user32.dll", SetLastError = true)]
    private static extern bool DestroyIcon(IntPtr handle);

    public TrayApp(bool autoStart = false)
    {
        _autoStartPending = autoStart;
        _rootDir    = FindRoot();
        _sidecarExe = Path.Combine(_rootDir, "src", "UMAnager.Sidecar", "bin", "Release", "net8.0-windows", "win-x86", "UMAnager.Sidecar.exe");
        _nexusExe   = Path.Combine(_rootDir, "src", "UMAnager.Nexus",   "bin", "Release", "net8.0",                "UMAnager.Nexus.exe");
        _pidFile    = Path.Combine(_rootDir, ".service-pids.json");
        var logDir  = Path.Combine(_rootDir, "logs");
        Directory.CreateDirectory(logDir);
        _sidecarLog = Path.Combine(logDir, "sidecar.log");
        _nexusLog   = Path.Combine(logDir, "nexus.log");

        // Discover any already-running services (started by us last session, or by launch-services.ps1).
        Rediscover();

        var menu = new ContextMenuStrip();
        _startItem   = new ToolStripMenuItem("Start services",   null, (_, _) => StartAll());
        _stopItem    = new ToolStripMenuItem("Stop services",    null, (_, _) => StopAll());
        _restartItem = new ToolStripMenuItem("Restart services", null, (_, _) => RestartAll());
        menu.Items.Add(_startItem);
        menu.Items.Add(_stopItem);
        menu.Items.Add(_restartItem);
        menu.Items.Add(new ToolStripSeparator());
        menu.Items.Add("Open dashboard", null, (_, _) => OpenUrl("http://localhost:5000"));
        menu.Items.Add("Open logs folder", null, (_, _) => OpenUrl(logDir));
        menu.Items.Add(new ToolStripSeparator());
        menu.Items.Add("Exit tray (leave services running)", null, (_, _) => ExitTray(stopServices: false));
        menu.Items.Add("Exit tray and stop services",        null, (_, _) => ExitTray(stopServices: true));

        _icon = new NotifyIcon
        {
            Icon             = SystemIcons.Application,
            Visible          = true,
            ContextMenuStrip = menu,
            Text             = "UMAnager (initializing)",
        };
        _icon.DoubleClick += (_, _) => OpenUrl("http://localhost:5000");

        _timer = new System.Windows.Forms.Timer { Interval = 3000 };
        _timer.Tick += (_, _) =>
        {
            if (_autoStartPending) { _autoStartPending = false; StartAll(); }
            RefreshStatus();
        };
        _timer.Start();

        RefreshStatus();
    }

    // ── Lifecycle ───────────────────────────────────────────────────────────

    private void StartAll()
    {
        try
        {
            // Re-scan first so we never launch a duplicate of an already-running instance,
            // regardless of how it was started (PS launcher, prior tray session, manual).
            Rediscover();

            if (!IsRunning(_sidecarProc))
                _sidecarProc = LaunchProcess(_sidecarExe, _sidecarLog);

            // Small delay so the Sidecar can open the named pipe before the Nexus tries to connect.
            Thread.Sleep(2000);

            if (!IsRunning(_nexusProc))
                _nexusProc = LaunchProcess(_nexusExe, _nexusLog);

            WritePidFile();
            RefreshStatus();
            _icon.ShowBalloonTip(2000, "UMAnager", "Services starting…", ToolTipIcon.Info);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to start services:\n{ex.Message}", "UMAnager Tray", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private void StopAll()
    {
        // Always re-scan: an earlier session may have left services that we never adopted.
        // We want to kill every UMAnager.Nexus / UMAnager.Sidecar process, not just our handles.
        Rediscover();
        KillAllByName("UMAnager.Nexus");
        KillAllByName("UMAnager.Sidecar");
        _sidecarProc = null;
        _nexusProc   = null;
        try { File.Delete(_pidFile); } catch { }
        RefreshStatus();
        _icon.ShowBalloonTip(1500, "UMAnager", "Services stopped.", ToolTipIcon.Info);
    }

    private static void KillAllByName(string processName)
    {
        foreach (var p in Process.GetProcessesByName(processName))
        {
            try
            {
                p.Kill(entireProcessTree: true);
                p.WaitForExit(3000);
            }
            catch { /* already gone */ }
            finally { p.Dispose(); }
        }
    }

    private void RestartAll()
    {
        StopAll();
        Thread.Sleep(800);
        StartAll();
    }

    private void ExitTray(bool stopServices)
    {
        _timer.Stop();
        if (stopServices) StopAll();
        _icon.Visible = false;
        _icon.Dispose();
        if (_currentHIcon != IntPtr.Zero) DestroyIcon(_currentHIcon);
        ExitThread();
    }

    // ── Process plumbing ────────────────────────────────────────────────────

    private static Process LaunchProcess(string exePath, string logPath)
    {
        if (!File.Exists(exePath))
            throw new FileNotFoundException($"Build artifact missing — run 'dotnet build -c Release' first:\n{exePath}");

        var psi = new ProcessStartInfo
        {
            FileName               = exePath,
            WorkingDirectory       = Path.GetDirectoryName(exePath)!,
            UseShellExecute        = false,
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
            CreateNoWindow         = true,
        };
        var proc = Process.Start(psi) ?? throw new InvalidOperationException($"Process.Start returned null for {exePath}");

        // Pipe both streams to the log file. Append-mode shared read so the WebUI's sidecar-log
        // endpoint and external tail viewers can still read while we write.
        _ = Task.Run(() => PipeToFile(proc.StandardOutput, logPath));
        _ = Task.Run(() => PipeToFile(proc.StandardError,  logPath));

        return proc;
    }

    private static async Task PipeToFile(StreamReader src, string logPath)
    {
        try
        {
            using var fs = new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            using var sw = new StreamWriter(fs);
            string? line;
            while ((line = await src.ReadLineAsync()) != null)
            {
                await sw.WriteLineAsync(line);
                await sw.FlushAsync();
            }
        }
        catch { /* process exited or log unwritable — ignore */ }
    }

    private static bool IsRunning(Process? proc)
    {
        if (proc is null) return false;
        try { proc.Refresh(); return !proc.HasExited; }
        catch { return false; }
    }

    // Source of truth: the OS process list. PID files are advisory — they can be stale, missing,
    // or refer to a recycled PID. Looking up by ProcessName means we always reflect reality and
    // never duplicate an already-running instance.
    private void Rediscover()
    {
        _sidecarProc = Process.GetProcessesByName("UMAnager.Sidecar").FirstOrDefault();
        _nexusProc   = Process.GetProcessesByName("UMAnager.Nexus").FirstOrDefault();
    }

    private void WritePidFile()
    {
        var payload = new Dictionary<string, int?>
        {
            ["sidecar"] = IsRunning(_sidecarProc) ? _sidecarProc!.Id : null,
            ["nexus"]   = IsRunning(_nexusProc)   ? _nexusProc!.Id   : null,
        };
        File.WriteAllText(_pidFile, JsonSerializer.Serialize(payload));
    }

    // ── Status / icon ───────────────────────────────────────────────────────

    private void RefreshStatus()
    {
        // Always rescan — picks up externally-started/killed processes without restarting the tray.
        Rediscover();
        var sideUp  = IsRunning(_sidecarProc);
        var nexusUp = IsRunning(_nexusProc);

        var sideText  = sideUp  ? $"running (PID {_sidecarProc!.Id})" : "stopped";
        var nexusText = nexusUp ? $"running (PID {_nexusProc!.Id})"   : "stopped";

        // NotifyIcon.Text is capped at 127 chars on older Windows; keep this short.
        var tip = $"Sidecar: {sideText}\nNexus:   {nexusText}";
        if (tip.Length > 127) tip = tip[..127];
        _icon.Text = tip;

        Color dot = (sideUp, nexusUp) switch
        {
            (true, true)  => Color.LimeGreen,
            (false, false) => Color.Crimson,
            _              => Color.Goldenrod,
        };
        SetIcon(dot);

        _startItem.Enabled   = !(sideUp && nexusUp);
        _stopItem.Enabled    = sideUp || nexusUp;
        _restartItem.Enabled = sideUp || nexusUp;
    }

    private void SetIcon(Color color)
    {
        using var bmp = new Bitmap(16, 16);
        using (var g = Graphics.FromImage(bmp))
        {
            g.SmoothingMode = SmoothingMode.AntiAlias;
            g.Clear(Color.Transparent);
            using var brush = new SolidBrush(color);
            g.FillEllipse(brush, 2, 2, 12, 12);
            using var pen = new Pen(Color.FromArgb(40, 40, 40), 1);
            g.DrawEllipse(pen, 2, 2, 12, 12);
        }

        var newHIcon = bmp.GetHicon();
        var oldIcon = _icon.Icon;
        _icon.Icon = Icon.FromHandle(newHIcon);
        if (_currentHIcon != IntPtr.Zero) DestroyIcon(_currentHIcon);
        _currentHIcon = newHIcon;
        // Don't dispose the framework-provided SystemIcons.Application on first call; subsequent
        // replacements are FromHandle wrappers, whose underlying HICONs we track in _currentHIcon.
        if (!ReferenceEquals(oldIcon, SystemIcons.Application)) oldIcon?.Dispose();
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static void OpenUrl(string target)
    {
        try { Process.Start(new ProcessStartInfo(target) { UseShellExecute = true }); }
        catch { /* shrug */ }
    }

    private static string FindRoot()
    {
        var dir = AppContext.BaseDirectory;
        while (!string.IsNullOrEmpty(dir))
        {
            if (File.Exists(Path.Combine(dir, "UMAnager.sln")))
                return dir;
            var parent = Path.GetDirectoryName(dir);
            if (parent == dir) break;
            dir = parent!;
        }
        throw new InvalidOperationException("Could not locate UMAnager.sln — is the tray exe inside the UMAnager2 tree?");
    }
}
