param(
    [string]$PayloadJson = '{}',
    [int]$DebugPort = 9222
)

$ErrorActionPreference = 'Stop'

function Write-JsonAndExit {
    param(
        [int]$Code,
        [string]$Status,
        [string]$Message,
        $Result = $null
    )

    $payload = @{
        status = $Status
        message = $Message
    }
    if ($null -ne $Result) {
        $payload.result = $Result
    }

    $payload | ConvertTo-Json -Depth 32 -Compress
    exit $Code
}

function Get-CdpTarget {
    param([int]$Port)

    $listUrl = "http://127.0.0.1:$Port/json/list"
    $targets = Invoke-RestMethod -Uri $listUrl -Method Get -TimeoutSec 2
    if ($null -eq $targets) { return $null }

    $candidate = $targets | Where-Object {
        $_.type -eq 'page' -and $_.url -match '^https://orepro\.netkeiba\.com/'
    } | Select-Object -First 1

    return $candidate
}

function Receive-CdpMessage {
    param([System.Net.WebSockets.ClientWebSocket]$Ws)

    $buffer = New-Object byte[] 32768
    $segment = New-Object System.ArraySegment[byte] -ArgumentList @(,$buffer)
    $builder = New-Object System.Text.StringBuilder

    do {
        $result = $Ws.ReceiveAsync($segment, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
        if ($result.Count -gt 0) {
            $chunk = [Text.Encoding]::UTF8.GetString($buffer, 0, $result.Count)
            [void]$builder.Append($chunk)
        }
    } while (-not $result.EndOfMessage)

    return $builder.ToString()
}

function Send-CdpCommand {
    param(
        [System.Net.WebSockets.ClientWebSocket]$Ws,
        [int]$Id,
        [string]$Method,
        [hashtable]$Params
    )

    $command = @{ id = $Id; method = $Method; params = $Params } | ConvertTo-Json -Depth 32 -Compress
    $bytes = [Text.Encoding]::UTF8.GetBytes($command)
    $segment = New-Object System.ArraySegment[byte] -ArgumentList @(,$bytes)
    [void]($Ws.SendAsync($segment, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, [Threading.CancellationToken]::None).GetAwaiter().GetResult())

    while ($true) {
        $raw = Receive-CdpMessage -Ws $Ws
        if (-not $raw) { continue }
        $obj = $raw | ConvertFrom-Json
        if ($null -ne $obj.id -and [int]$obj.id -eq $Id) {
            return $obj
        }
    }
}

try {
    $target = Get-CdpTarget -Port $DebugPort
    if (-not $target -or -not $target.webSocketDebuggerUrl) {
        Write-JsonAndExit -Code 1 -Status 'error' -Message 'No managed OrePro companion tab found on the debug port. Click Open OrePro once to create one.'
    }

    $ws = [System.Net.WebSockets.ClientWebSocket]::new()
    [void]($ws.ConnectAsync([Uri]$target.webSocketDebuggerUrl, [Threading.CancellationToken]::None).GetAwaiter().GetResult())

    [void](Send-CdpCommand -Ws $ws -Id 1 -Method 'Runtime.enable' -Params @{})

    $payloadEscaped = $PayloadJson.Replace('\', '\\').Replace("`r", '').Replace("`n", '').Replace("'", "\\'")

    $expressionTemplate = @'
(async () => {
  const payload = JSON.parse('__PAYLOAD__');
  const symbolToCode = {'◎': 1, '〇': 2, '▲': 3, '△': 4};
  const labels = {1: '◎', 2: '〇', 3: '▲', 4: '△'};
  const results = [];

  const decodeJsonp = (txt) => {
    const raw = String(txt || '').trim();
    if (raw.startsWith('(') && raw.endsWith(')')) {
      try { return JSON.parse(raw.slice(1, -1)); } catch { return {}; }
    }
    const p1 = raw.indexOf('(');
    const p2 = raw.lastIndexOf(')');
    if (p1 >= 0 && p2 > p1) {
      try { return JSON.parse(raw.slice(p1 + 1, p2)); } catch { return {}; }
    }
    try { return JSON.parse(raw); } catch { return {}; }
  };

  for (const race of (payload.races || [])) {
    const raceId = String(race.race_id || '').trim();
    if (!raceId) {
      results.push({ raceId: '', status: 'error', message: 'race_id is required', requested: [], resolved: [] });
      continue;
    }

    const dedupByCode = {};
    for (const m of (race.marks || [])) {
      const directCode = Number(m.mark_code || m.code || 0);
      const code = [1,2,3,4].includes(directCode) ? directCode : symbolToCode[String(m.symbol || '').trim()];
      const post = parseInt(m.post, 10);
      if (!code || !Number.isFinite(post) || post <= 0) continue;
      if (!(code in dedupByCode)) dedupByCode[code] = post;
    }

    const requested = Object.entries(dedupByCode)
      .sort((a, b) => Number(a[0]) - Number(b[0]))
      .map(([code, post]) => ({ symbol: labels[code], post }));

    if (!requested.length) {
      results.push({ raceId, status: 'skipped', message: 'No valid main marks (1-4) to apply for this race.', requested: [], resolved: [], unmatchedPosts: [] });
      continue;
    }

    let shutubaHtml = '';
    try {
      const shutubaRes = await fetch(`https://orepro.netkeiba.com/bet/shutuba.html?race_id=${encodeURIComponent(raceId)}`, {
        method: 'GET',
        credentials: 'include'
      });
      shutubaHtml = await shutubaRes.text();
    } catch (err) {
      results.push({ raceId, status: 'error', message: `Failed fetching shutuba in companion session: ${err?.message || err}`, requested, resolved: [], unmatchedPosts: requested.map(r => r.post) });
      continue;
    }

    const doc = new DOMParser().parseFromString(shutubaHtml, 'text/html');
    const postToSeq = {};
    doc.querySelectorAll("tr.HorseList[id^='tr_']").forEach(row => {
      const id = String(row.id || '');
      const m = id.match(/^tr_(\d+)$/);
      if (!m) return;
      const seq = Number(m[1]);
      const postCell = row.querySelector("td[id^='act_waku_']");
      if (!postCell) return;
      const post = Number(String(postCell.textContent || '').replace(/\D/g, ''));
      if (post > 0) postToSeq[post] = seq;
    });

    const resolved = [];
    const unmatchedPosts = [];
    for (const [codeStr, post] of Object.entries(dedupByCode).sort((a, b) => Number(a[0]) - Number(b[0]))) {
      const code = Number(codeStr);
      const seq = postToSeq[post];
      if (!seq) {
        unmatchedPosts.push(post);
        continue;
      }
      resolved.push({ symbol: labels[code], post, seq, markCode: code });
    }

    if (!resolved.length) {
      results.push({ raceId, status: 'error', message: 'None of the requested post numbers were found in OrePro shutuba rows.', requested, resolved: [], unmatchedPosts });
      continue;
    }

    if (payload.dry_run) {
      results.push({ raceId, status: 'dry-run', message: 'Dry run only. No OrePro cart updates were sent.', requested, resolved, unmatchedPosts });
      continue;
    }

    const params = new URLSearchParams();
    params.set('input', 'UTF-8');
    params.set('output', 'json');
    params.set('action', 'replace');
    params.set('group', `oremark_${raceId}`);
    for (const row of resolved) {
      params.append('item_id[]', String(row.seq));
      params.append('item_value[]', '1');
      params.append('item_price[]', '0');
      params.append('client_data[]', `_${row.markCode}`);
    }

    try {
      const cartRes = await fetch('https://orepro.netkeiba.com/cart/', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
        body: params.toString()
      });
      const cartText = await cartRes.text();
      let cartResponse = null;
      try { cartResponse = JSON.parse(cartText); } catch { cartResponse = { raw: cartText.slice(0, 1000) }; }

      await fetch('https://orepro.netkeiba.com/bet/api_post_bet_generator.html', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
        body: new URLSearchParams({ input: 'UTF-8', output: 'jsonp', race_id: raceId }).toString()
      });

      const viewRes = await fetch('https://orepro.netkeiba.com/bet/api_get_bet_view.html', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
        body: new URLSearchParams({ input: 'UTF-8', output: 'jsonp', race_id: raceId, src: 'session' }).toString()
      });
      const viewTxt = await viewRes.text();
      const viewJson = decodeJsonp(viewTxt);
      const previewHtml = String((viewJson && viewJson.data) || '');
      const previewText = new DOMParser().parseFromString(previewHtml, 'text/html').body?.textContent || '';
      const betPreviewLines = previewText.split('\n').map(v => v.trim()).filter(Boolean).slice(0, 24);

      results.push({
        raceId,
        status: 'ok',
        message: 'Marks applied through companion window session (no money action).',
        requested,
        resolved,
        unmatchedPosts,
        cartResponse,
        betPreviewLines
      });
    } catch (err) {
      results.push({
        raceId,
        status: 'error',
        message: `Failed applying marks through companion window session: ${err?.message || err}`,
        requested,
        resolved,
        unmatchedPosts
      });
    }
  }

  const okCount = results.filter(r => r.status === 'ok').length;
  if (!payload.dry_run && okCount > 0 && payload.force_refresh !== false) {
    setTimeout(() => {
      try { window.location.reload(); } catch (_) {}
    }, 120);
  }

  return {
    status: okCount > 0 ? 'ok' : 'warn',
    dryRun: !!payload.dry_run,
    message: `Applied marks for ${okCount}/${results.length} races via companion window session. This only updates mark/cart state and does not submit paid bets.`,
    results
  };
})()
'@

    $expression = $expressionTemplate.Replace('__PAYLOAD__', $payloadEscaped)

    $eval = Send-CdpCommand -Ws $ws -Id 2 -Method 'Runtime.evaluate' -Params @{
        expression = $expression
        awaitPromise = $true
        returnByValue = $true
    }

    $ws.Dispose()

    if ($eval.error) {
        Write-JsonAndExit -Code 1 -Status 'error' -Message ("CDP evaluate failed: " + ($eval.error.message | Out-String))
    }

    $resultValue = $eval.result.result.value
    if ($null -eq $resultValue) {
        Write-JsonAndExit -Code 1 -Status 'error' -Message 'No result returned from companion tab execution.'
    }

    $resultValue | ConvertTo-Json -Depth 32 -Compress
    exit 0
} catch {
    Write-JsonAndExit -Code 1 -Status 'error' -Message ("Companion vote apply helper failed: " + $_.Exception.Message)
}
