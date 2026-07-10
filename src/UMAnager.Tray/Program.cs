// ============================================================
// FILE: Program.cs  (UMAnager.Tray)
// LAYER: Entry point (WinForms tray supervisor)
// PURPOSE: STAThread Main with a single-instance mutex; launches TrayApp.
// LAST DOCUMENTED: 2026-06-02
// ============================================================
using System.Runtime.InteropServices;

namespace UMAnager.Tray;

internal static class Program
{
    [STAThread]
    private static void Main(string[] args)
    {
        // Single-instance guard so the tray doesn't get duplicated on accidental re-launch.
        using var mutex = new Mutex(initiallyOwned: true, name: "UMAnager.Tray.SingleInstance", out var createdNew);
        if (!createdNew) return;

        var autoStart = args.Contains("--autostart", StringComparer.OrdinalIgnoreCase);
        ApplicationConfiguration.Initialize();
        Application.Run(new TrayApp(autoStart));
    }
}
