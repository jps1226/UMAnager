using System.Runtime.InteropServices;

namespace UMAnager.Tray;

internal static class Program
{
    [STAThread]
    private static void Main()
    {
        // Single-instance guard so the tray doesn't get duplicated on accidental re-launch.
        using var mutex = new Mutex(initiallyOwned: true, name: "UMAnager.Tray.SingleInstance", out var createdNew);
        if (!createdNew) return;

        ApplicationConfiguration.Initialize();
        Application.Run(new TrayApp());
    }
}
