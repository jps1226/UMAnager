using System.Runtime.InteropServices;

namespace UMAnager.Sidecar.Dialogs;

/// <summary>
/// P/Invoke-based dialog detector for JV-Link setup/configuration dialogs.
/// Monitors for the "CD/ROM setup" dialog that appears after JVOpen.
/// The user is expected to manually interact with the dialog (click "No" to proceed).
/// </summary>
internal static class DialogHelper
{
    [DllImport("user32.dll", SetLastError = true)]
    private static extern IntPtr FindWindow(string? lpClassName, string? lpWindowName);

    [DllImport("user32.dll", SetLastError = true)]
    private static extern IntPtr FindWindowEx(IntPtr hwndParent, IntPtr hwndChildAfter, string? lpClassName, string? lpWindowName);

    [DllImport("user32.dll")]
    private static extern bool IsWindowVisible(IntPtr hWnd);

    [DllImport("user32.dll", SetLastError = true)]
    private static extern int GetWindowText(IntPtr hWnd, System.Text.StringBuilder lpString, int nMaxCount);

    [DllImport("user32.dll")]
    private static extern bool EnumChildWindows(IntPtr hWndParent, EnumWindowsProc lpEnumFunc, IntPtr lParam);

    private delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);

    /// <summary>
    /// Waits for the JV-Link setup dialog to be dismissed by the user.
    /// Polls for the dialog window and logs when it appears.
    /// The user is expected to interact with the dialog (click "No") manually.
    /// </summary>
    /// <param name="timeoutSeconds">Maximum time to wait for dialog to be dismissed (default 90 seconds).</param>
    /// <returns>True if dialog was detected; False if no dialog found within timeout.</returns>
    public static bool WaitForDialogDismissal(int timeoutSeconds = 90)
    {
        var startTime = DateTime.UtcNow;
        const int pollIntervalMs = 500; // Check every 500ms to reduce CPU usage
        bool dialogDetected = false;

        Console.WriteLine($"[DialogHelper] Monitoring for JV-Link setup dialog (up to {timeoutSeconds}s)...");
        Console.WriteLine("[DialogHelper] ⚠️  If a dialog appears, please click the 'No' button to proceed.");

        while ((DateTime.UtcNow - startTime).TotalSeconds < timeoutSeconds)
        {
            IntPtr dialogHwnd = FindDialogWindow();

            if (dialogHwnd != IntPtr.Zero && !dialogDetected)
            {
                dialogDetected = true;
                Console.WriteLine($"[DialogHelper] ✓ Dialog detected! Waiting for you to dismiss it...");
            }

            // If dialog was detected but is no longer visible, it's been dismissed
            if (dialogDetected && dialogHwnd == IntPtr.Zero)
            {
                Console.WriteLine($"[DialogHelper] ✓ Dialog dismissed. Proceeding with data read...");
                return true;
            }

            System.Threading.Thread.Sleep(pollIntervalMs);
        }

        if (dialogDetected)
        {
            Console.WriteLine($"[DialogHelper] ⚠️  Dialog was detected but not dismissed within {timeoutSeconds}s timeout.");
            // Timeout, but dialog was at least detected
            return true;
        }

        Console.WriteLine($"[DialogHelper] No dialog detected within {timeoutSeconds}s (proceeding without dialog).");
        return false;
    }

    /// <summary>
    /// Finds the JV-Link setup dialog window by searching for known titles.
    /// </summary>
    private static IntPtr FindDialogWindow()
    {
        // Search for windows with common JV-Link dialog titles
        string[] possibleTitles = new[]
        {
            "JV-Link", // Generic JV-Link window
            "Setup",   // Setup dialog
            "設定",    // Japanese for "Setup"
            "JRA-VAN", // Could be titled with this
        };

        foreach (var title in possibleTitles)
        {
            IntPtr hwnd = FindWindow(null, title);
            if (hwnd != IntPtr.Zero && IsWindowVisible(hwnd))
            {
                // Verify this is a dialog by checking its window class
                var className = GetWindowClassName(hwnd);
                if (className.Contains("#32770") || className.Contains("Dialog"))
                {
                    return hwnd;
                }
            }
        }

        // If no exact match, enumerate all visible top-level windows and look for setup-like dialogs
        return FindDialogByEnumeration();
    }

    /// <summary>
    /// Enumerates all visible windows to find one that looks like a JV-Link setup dialog.
    /// </summary>
    private static IntPtr FindDialogByEnumeration()
    {
        IntPtr foundWindow = IntPtr.Zero;
        var collected = new List<(IntPtr hwnd, string title)>();

        EnumWindowsProc callback = (hwnd, lParam) =>
        {
            if (!IsWindowVisible(hwnd))
                return true; // Continue enumeration

            var sb = new System.Text.StringBuilder(256);
            GetWindowText(hwnd, sb, 256);
            var title = sb.ToString();

            // Look for windows with "setup", "配置", or other setup-related keywords
            if (title.Contains("Setup", StringComparison.OrdinalIgnoreCase) ||
                title.Contains("JV-Link", StringComparison.OrdinalIgnoreCase) ||
                title.Contains("JRA-VAN", StringComparison.OrdinalIgnoreCase) ||
                title.Contains("配置") ||
                title.Contains("セットアップ"))
            {
                var className = GetWindowClassName(hwnd);
                if (className.Contains("#32770") || title.Contains("Dialog"))
                {
                    collected.Add((hwnd, title));
                }
            }

            return true; // Continue enumeration
        };

        EnumChildWindows(IntPtr.Zero, callback, IntPtr.Zero);

        // Return the first likely dialog found
        if (collected.Count > 0)
        {
            foundWindow = collected[0].hwnd;
            Console.WriteLine($"[DialogHelper] Enumeration found window: '{collected[0].title}'");
        }

        return foundWindow;
    }

    /// <summary>
    /// Gets the window class name (used to identify dialog windows).
    /// </summary>
    private static string GetWindowClassName(IntPtr hwnd)
    {
        var sb = new System.Text.StringBuilder(256);
        GetClassName(hwnd, sb, 256);
        return sb.ToString();
    }

    [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
    private static extern int GetClassName(IntPtr hWnd, System.Text.StringBuilder lpClassName, int nMaxCount);

}
