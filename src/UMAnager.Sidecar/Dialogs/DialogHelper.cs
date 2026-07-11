// ============================================================
// FILE: DialogHelper.cs
// LAYER: Sidecar (Win32 P/Invoke)
// PURPOSE: Auto-dismisses the JV-Link "セットアップ" (start-kit CD/DVD) dialog that JV-Link pops on
//          the STA thread during JVInit/JVOpen. That dialog BLOCKS the Sidecar's single COM thread
//          until a human clicks it, so a background watcher thread polls for it and clicks the
//          "no CD" radio (id 228) then OK (id 1) — exactly the manual dismissal the operator used
//          to perform by hand on every Sidecar restart.
// CAUTION: The watcher deliberately runs OFF the STA thread. When the dialog is up, the STA thread
//          is parked inside the COM call, so only another thread can reach in and clear it.
// LAST DOCUMENTED: 2026-07-10
// ============================================================
using System.Runtime.InteropServices;
using System.Text;

namespace UMAnager.Sidecar.Dialogs;

/// <summary>
/// Win32 P/Invoke auto-dismisser for the JV-Link setup dialog. Start it once at Sidecar startup;
/// it runs for the life of the process on its own background thread.
/// </summary>
internal static class DialogHelper
{
    [DllImport("user32.dll")] private static extern bool EnumWindows(EnumProc cb, IntPtr l);
    [DllImport("user32.dll", CharSet = CharSet.Unicode)] private static extern int GetClassName(IntPtr h, StringBuilder s, int n);
    [DllImport("user32.dll", CharSet = CharSet.Unicode)] private static extern int GetWindowText(IntPtr h, StringBuilder s, int n);
    [DllImport("user32.dll")] private static extern bool IsWindowVisible(IntPtr h);
    [DllImport("user32.dll")] private static extern IntPtr GetDlgItem(IntPtr hDlg, int id);
    [DllImport("user32.dll", CharSet = CharSet.Auto)] private static extern IntPtr SendMessage(IntPtr h, uint msg, IntPtr wp, IntPtr lp);
    [DllImport("user32.dll")] private static extern uint IsDlgButtonChecked(IntPtr hDlg, int id);

    private delegate bool EnumProc(IntPtr h, IntPtr l);

    private const uint BM_CLICK = 0x00F5;
    private const string DialogClass = "#32770";      // standard Win32 dialog window class
    private const int RadioNoCd = 228;                // "スタートキット(CD/DVD-ROM)を持っていない"
    private const int RadioHasCd = 227;               // "…を持っている（推奨）"
    private const int BtnOk = 1;                       // IDOK
    private const string NoCdMarker = "持っていない"; // label check on radio 228, so we never OK a look-alike

    /// <summary>
    /// Spawns a background thread that watches for the JV-Link setup dialog for the life of the
    /// process and auto-clicks "no CD" + OK whenever it appears. Safe to call once at startup.
    /// </summary>
    public static void StartAutoDismissWatcher(CancellationToken ct)
    {
        var t = new Thread(() => WatchLoop(ct))
        {
            IsBackground = true,
            Name = "JvSetupDialogWatcher",
        };
        t.Start();
        Console.WriteLine("[DialogHelper] JV-Link setup-dialog auto-dismiss watcher started.");
    }

    private static void WatchLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var hwnd = FindSetupDialog();
                if (hwnd != IntPtr.Zero)
                {
                    DismissSetupDialog(hwnd);
                    Thread.Sleep(1500); // cooldown so we don't re-click while it tears down
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[DialogHelper] watcher error: {ex.Message}");
            }

            Thread.Sleep(500);
        }
    }

    /// <summary>Finds the JV-Link setup dialog, or IntPtr.Zero if it isn't on screen.</summary>
    private static IntPtr FindSetupDialog()
    {
        IntPtr found = IntPtr.Zero;
        EnumProc cb = (h, l) =>
        {
            if (!IsWindowVisible(h)) return true;

            var cls = new StringBuilder(64);
            GetClassName(h, cls, cls.Capacity);
            if (cls.ToString() != DialogClass) return true;

            // Must carry both radios + OK to be our dialog…
            if (GetDlgItem(h, RadioNoCd) == IntPtr.Zero) return true;
            if (GetDlgItem(h, RadioHasCd) == IntPtr.Zero) return true;
            var ok = GetDlgItem(h, BtnOk);
            if (ok == IntPtr.Zero) return true;

            // …and the "no CD" radio's label must match, so we never OK a look-alike #32770 dialog.
            var label = new StringBuilder(256);
            GetWindowText(GetDlgItem(h, RadioNoCd), label, label.Capacity);
            if (!label.ToString().Contains(NoCdMarker)) return true;

            found = h;
            return false; // stop enumeration
        };
        EnumWindows(cb, IntPtr.Zero);
        return found;
    }

    /// <summary>Selects the "no CD" radio then clicks OK — the operator's manual dismissal.</summary>
    private static void DismissSetupDialog(IntPtr hwnd)
    {
        var radio = GetDlgItem(hwnd, RadioNoCd);
        var ok = GetDlgItem(hwnd, BtnOk);
        if (radio == IntPtr.Zero || ok == IntPtr.Zero) return;

        Console.WriteLine("[DialogHelper] JV-Link setup dialog detected — selecting 'no CD' + OK.");
        SendMessage(radio, BM_CLICK, IntPtr.Zero, IntPtr.Zero);
        Thread.Sleep(250);

        if (IsDlgButtonChecked(hwnd, RadioNoCd) == 0)
            Console.Error.WriteLine("[DialogHelper] 'no CD' radio did not register as checked; clicking OK anyway.");

        SendMessage(ok, BM_CLICK, IntPtr.Zero, IntPtr.Zero);
        Console.WriteLine("[DialogHelper] JV-Link setup dialog dismissed.");
    }
}
