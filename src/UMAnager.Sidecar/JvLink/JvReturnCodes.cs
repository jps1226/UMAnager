namespace UMAnager.Sidecar.JvLink;

/// <summary>
/// Well-known JV-Link return codes we branch on explicitly. (Oracle 2026-06-07.)
/// </summary>
internal static class JvReturnCodes
{
    /// <summary>
    /// <c>-504</c> サーバーメンテナンス中 — the JRA-VAN server is under maintenance. Returned by
    /// both <c>JVOpen</c> and <c>JVRTOpen</c>. Fully distinct from <c>-1</c> (該当データ無し / no
    /// applicable data) and from transient/network errors like <c>-502</c> (ダウンロード失敗).
    /// Not observable at <c>JVInit</c> (which never contacts the server) — only on a stream open.
    /// </summary>
    public const int Maintenance = -504;

    /// <summary>
    /// Sentinel we report back over the pipe in a <c>*_COMPLETE</c> message's <c>record_count</c>
    /// to tell the Nexus "this stream aborted because the server is in maintenance" — as opposed to
    /// <c>-1</c>, which the Nexus treats as a generic stream error. The Nexus flags a back-off state
    /// on this value and clears it on the next successful (>= 0) completion.
    /// </summary>
    public const int MaintenanceStored = -504;
}
