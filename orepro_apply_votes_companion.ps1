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
    try {
        $targets = Invoke-RestMethod -Uri $listUrl -Method Get -TimeoutSec 2
    } catch {
        return $null
    }
    if ($null -eq $targets) { return $null }

    $candidate = $targets | Where-Object {
        $_.type -eq 'page' -and $_.url -match '^https://orepro\.netkeiba\.com/'
    } | Sort-Object -Property @{
        Expression = {
            $url = [string]$_.url
            if ($url -match '/bet/shutuba\.html') { 0 }
            elseif ($url -match '/bet/') { 1 }
            else { 2 }
        }
    }, @{
        Expression = { [string]$_.title }
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

function Get-CdpLocationHref {
    param(
        [System.Net.WebSockets.ClientWebSocket]$Ws,
        [int]$Id
    )

    $resp = Send-CdpCommand -Ws $Ws -Id $Id -Method 'Runtime.evaluate' -Params @{
        expression = 'String(location.href || "")'
        returnByValue = $true
        awaitPromise = $false
    }

    return [string]($resp.result.result.value | Out-String).Trim()
}

function Wait-CdpLocation {
    param(
        [System.Net.WebSockets.ClientWebSocket]$Ws,
        [string]$Pattern,
        [int]$MaxWaitMs = 10000,
        [int]$PollMs = 300,
        [int]$StartId = 100
    )

    $deadline = [DateTime]::UtcNow.AddMilliseconds([Math]::Max($MaxWaitMs, 0))
    $id = $StartId

    while ([DateTime]::UtcNow -lt $deadline) {
        $href = ''
        try {
            $href = Get-CdpLocationHref -Ws $Ws -Id $id
        } catch {
            $href = ''
        }

        if ($href -and $href -match $Pattern) {
            return $href
        }

        Start-Sleep -Milliseconds $PollMs
        $id += 1
    }

    return ''
}

try {
    $target = $null
    for ($attempt = 0; $attempt -lt 12; $attempt++) {
        $target = Get-CdpTarget -Port $DebugPort
        if ($target -and $target.webSocketDebuggerUrl) {
            break
        }
        Start-Sleep -Milliseconds 500
    }

    if (-not $target -or -not $target.webSocketDebuggerUrl) {
        Write-JsonAndExit -Code 1 -Status 'error' -Message 'No managed OrePro companion tab found on the debug port. Click Open OrePro once, wait a moment for the page to finish opening, then retry Apply Votes.'
    }

    $ws = [System.Net.WebSockets.ClientWebSocket]::new()
    [void]($ws.ConnectAsync([Uri]$target.webSocketDebuggerUrl, [Threading.CancellationToken]::None).GetAwaiter().GetResult())

    [void](Send-CdpCommand -Ws $ws -Id 1 -Method 'Runtime.enable' -Params @{})

    $payloadBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($PayloadJson))

    $expressionTemplate = @'
(async () => {
  const decodeUtf8Base64 = (b64) => {
    const binary = atob(String(b64 || ''));
    const bytes = Uint8Array.from(binary, ch => ch.charCodeAt(0));
    return new TextDecoder('utf-8').decode(bytes);
  };

  let payload;
  try {
    payload = JSON.parse(decodeUtf8Base64('__PAYLOAD_B64__'));
  } catch (err) {
    return {
      status: 'error',
      dryRun: false,
      message: `Could not decode companion vote payload: ${err?.message || err}`,
      results: []
    };
  }

  const symbolToCode = {'\u25ce': 1, '\u3007': 2, '\u25b2': 3, '\u25b3': 4};
  const labels = {1: '\u25ce', 2: '\u3007', 3: '\u25b2', 4: '\u25b3'};
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

    const singleByCode = {};
    const extraTriangles = [];
    const seenPairs = new Set();

    for (const m of (race.marks || [])) {
      const directCode = Number(m.mark_code || m.code || 0);
      const code = [1,2,3,4].includes(directCode) ? directCode : symbolToCode[String(m.symbol || '').trim()];
      const post = parseInt(m.post, 10);
      if (!code || !Number.isFinite(post) || post <= 0) continue;

      const pairKey = `${code}:${post}`;
      if (seenPairs.has(pairKey)) continue;
      seenPairs.add(pairKey);

      if (code === 4) {
        extraTriangles.push({ code, post });
      } else if (!(code in singleByCode)) {
        singleByCode[code] = post;
      }
    }

    const requested = [
      ...Object.entries(singleByCode).map(([code, post]) => ({ symbol: labels[code], post })),
      ...extraTriangles.map(item => ({ symbol: labels[item.code], post: item.post }))
    ].sort((a, b) => {
      const aCode = symbolToCode[a.symbol] || 99;
      const bCode = symbolToCode[b.symbol] || 99;
      if (aCode !== bCode) return aCode - bCode;
      return Number(a.post || 0) - Number(b.post || 0);
    });

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
    let nextRaceUrl = '';
    try {
      const nextHref = doc.querySelector('.RaceNumLink .Next_Race a')?.getAttribute('href') || '';
      if (nextHref) {
        nextRaceUrl = new URL(nextHref, 'https://orepro.netkeiba.com').toString();
      }
    } catch (_) {}

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
    for (const reqMark of requested) {
      const code = symbolToCode[reqMark.symbol];
      const post = Number(reqMark.post);
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
      results.push({ raceId, status: 'dry-run', message: 'Dry run only. No OrePro cart updates were sent.', requested, resolved, unmatchedPosts, nextRaceUrl });
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

      let pageUpdated = false;
      try {
        const currentRaceMatch = /[?&]race_id=([^&]+)/.exec(String(location.search || ''));
        const currentRaceId = currentRaceMatch ? decodeURIComponent(currentRaceMatch[1]) : '';
        if (/\/bet\/shutuba\.html/.test(String(location.pathname || '')) &&
            currentRaceId === raceId &&
            typeof initial_animate_mark === 'function' &&
            typeof animate_mark === 'function') {
          initial_animate_mark();
          resolved.forEach(row => animate_mark(String(row.seq), String(row.markCode)));
          pageUpdated = true;

          if (typeof get_baken_image === 'function' && typeof url_baken_image !== 'undefined') {
            await new Promise(resolve => {
              try {
                get_baken_image(url_baken_image, raceId, 'session', function(data) {
                  try {
                    const imageNode = document.getElementById('baken_image');
                    if (imageNode && data && typeof data.data === 'string') {
                      imageNode.innerHTML = data.data;
                    }
                  } catch (_) {}
                  resolve();
                });
              } catch (_) {
                resolve();
              }
            });
          }
        }
      } catch (_) {}

      results.push({
        raceId,
        status: 'ok',
        message: 'Marks applied through companion window session.',
        requested,
        resolved,
        unmatchedPosts,
        cartResponse,
        betPreviewLines,
        pageUpdated,
        nextRaceUrl
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
  const lastOkResult = [...results].reverse().find(r => r.status === 'ok' && r.raceId) || null;
  const lastOkRace = lastOkResult?.raceId || '';
  const nextRaceUrl = lastOkResult?.nextRaceUrl || '';

  if (!payload.dry_run && okCount > 0 && !payload.submit_after_apply && payload.force_refresh !== false) {
    setTimeout(() => {
      try {
        const currentRaceMatch = /[?&]race_id=([^&]+)/.exec(String(location.search || ''));
        const currentRaceId = currentRaceMatch ? decodeURIComponent(currentRaceMatch[1]) : '';
        const onMatchingShutuba = lastOkRace &&
          /\/bet\/shutuba\.html/.test(String(location.pathname || '')) &&
          currentRaceId === lastOkRace;

        if (lastOkRace && !onMatchingShutuba) {
          window.location.href = `https://orepro.netkeiba.com/bet/shutuba.html?race_id=${encodeURIComponent(lastOkRace)}&mode=init`;
        } else {
          window.location.reload();
        }
      } catch (_) {}
    }, 120);
  }

  return {
    status: okCount > 0 ? 'ok' : 'warn',
    dryRun: !!payload.dry_run,
    message: `Applied marks for ${okCount}/${results.length} races via companion window session.${lastOkRace ? ` Ready on race ${lastOkRace}.` : ''}`,
    results,
    lastOkRace,
    nextRaceUrl,
    submitRequested: !!payload.submit_after_apply,
    goNextRequested: payload.go_next_race !== false
  };
})()
'@

    $expression = $expressionTemplate.Replace('__PAYLOAD_B64__', $payloadBase64)

    $eval = Send-CdpCommand -Ws $ws -Id 2 -Method 'Runtime.evaluate' -Params @{
        expression = $expression
        awaitPromise = $true
        returnByValue = $true
    }

    if ($eval.error) {
        try { $ws.Dispose() } catch {}
        Write-JsonAndExit -Code 1 -Status 'error' -Message ("CDP evaluate failed: " + ($eval.error.message | Out-String))
    }

    $resultValue = $eval.result.result.value
    if ($null -eq $resultValue) {
        try { $ws.Dispose() } catch {}
        Write-JsonAndExit -Code 1 -Status 'error' -Message 'No result returned from companion tab execution.'
    }

    $lastOkRace = [string]($resultValue.lastOkRace | Out-String).Trim()
    $nextRaceUrl = [string]($resultValue.nextRaceUrl | Out-String).Trim()
    $submitRequested = $false
    $goNextRequested = $false
    try { $submitRequested = [bool]$resultValue.submitRequested } catch {}
    try { $goNextRequested = [bool]$resultValue.goNextRequested } catch {}

    if (-not $resultValue.dryRun -and $submitRequested -and $lastOkRace) {
        $submitFlow = [ordered]@{
            raceId = $lastOkRace
            submitRequested = $submitRequested
            goNextRequested = $goNextRequested
            submitStatus = 'pending'
            submitMessage = ''
            receiptUrl = ''
            nextRaceUrl = $nextRaceUrl
            nextStatus = if ($goNextRequested) { 'pending' } else { 'skipped' }
            nextMessage = ''
            nextLandingUrl = ''
        }

        try {
            [void](Send-CdpCommand -Ws $ws -Id 3 -Method 'Page.enable' -Params @{})

            $targetRaceUrl = "https://orepro.netkeiba.com/bet/shutuba.html?race_id=$([Uri]::EscapeDataString($lastOkRace))&mode=init"
            $currentUrl = Get-CdpLocationHref -Ws $ws -Id 4
            [void](Send-CdpCommand -Ws $ws -Id 5 -Method 'Page.navigate' -Params @{ url = $targetRaceUrl })
            $landedUrl = Wait-CdpLocation -Ws $ws -Pattern "shutuba\.html.*race_id=$([regex]::Escape($lastOkRace))" -MaxWaitMs 12000 -PollMs 400 -StartId 6
            if ($landedUrl) {
                $currentUrl = $landedUrl
            }
            Start-Sleep -Milliseconds 900

            $prepareSubmitExpressionTemplate = @'
(async () => {
  const raceId = '__RACE_ID__';
  const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  const snapshot = () => {
    const imageNode = document.getElementById('baken_image');
    const noneNode = document.getElementById('baken_image_none');
    const previewText = String((imageNode && (imageNode.innerText || imageNode.textContent || imageNode.innerHTML)) || '').trim();
    return {
      readyState: document.readyState,
      hasPreview: !!previewText,
      hasNone: !!noneNode,
      previewText: previewText.slice(0, 200)
    };
  };

  for (let i = 0; i < 40; i++) {
    if (document.readyState === 'complete') break;
    await wait(250);
  }

  try {
    if (typeof initial_animate_mark === 'function') {
      initial_animate_mark();
    }
  } catch (_) {}

  const refreshPreview = () => new Promise(resolve => {
    try {
      if (typeof get_baken_image === 'function' && typeof url_baken_image !== 'undefined') {
        get_baken_image(url_baken_image, raceId, 'session', function(data) {
          try {
            const imageNode = document.getElementById('baken_image');
            if (imageNode && data && typeof data.data === 'string') {
              imageNode.innerHTML = data.data;
            }
          } catch (_) {}
          resolve(true);
        });
        return;
      }
    } catch (_) {}
    resolve(false);
  });

  try {
    if (typeof bet_generator === 'function' && typeof url_gen !== 'undefined') {
      await new Promise(resolve => {
        try {
          bet_generator(url_gen, raceId, function() { resolve(true); });
        } catch (_) {
          resolve(false);
        }
      });
    }
  } catch (_) {}

  await refreshPreview();

  for (let i = 0; i < 48; i++) {
    const state = snapshot();
    if (state.hasPreview && !state.hasNone) {
      return { status: 'ok', message: 'Vote preview is ready after page refresh.', state };
    }
    await wait(250);
    if (i === 11 || i === 23 || i === 35) {
      await refreshPreview();
    }
  }

  const finalState = snapshot();
  if (finalState.hasPreview && !finalState.hasNone) {
    return { status: 'ok', message: 'Vote preview is ready after page refresh.', state: finalState };
  }
  return { status: 'warn', message: 'Timed out waiting for the refreshed OrePro page to show the generated vote preview.', state: finalState };
})()
'@
            $prepareSubmitExpression = $prepareSubmitExpressionTemplate.Replace('__RACE_ID__', $lastOkRace)
            $prepareResp = Send-CdpCommand -Ws $ws -Id 18 -Method 'Runtime.evaluate' -Params @{
                expression = $prepareSubmitExpression
                awaitPromise = $true
                returnByValue = $true
            }

            $prepareValue = $prepareResp.result.result.value
            if (-not $prepareValue -or $prepareValue.status -ne 'ok') {
                $submitFlow.submitStatus = 'warn'
                $submitFlow.submitMessage = if ($prepareValue) { [string]($prepareValue.message | Out-String).Trim() } else { 'Could not confirm that the vote preview was ready after the page refresh.' }
                if ($goNextRequested) {
                    $submitFlow.nextStatus = 'skipped'
                    $submitFlow.nextMessage = 'Skipped next-race navigation because submit was not confirmed safe yet.'
                }
            } else {
                $submitExpressionTemplate = @'
(() => {
  const raceId = '__RACE_ID__';
  const btn = document.getElementById(`act-bet_${raceId}`) || document.querySelector(`#act-bet_${raceId}`);
  if (!btn) {
    return { status: 'error', message: 'Submit button not found on the OrePro race page.' };
  }
  try {
    if (window.jQuery) {
      window.jQuery(btn).trigger('click');
    } else {
      btn.click();
    }
    return { status: 'ok', message: 'Submit button clicked after preview refresh.' };
  } catch (err) {
    return { status: 'error', message: `Submit click failed: ${err?.message || err}` };
  }
})()
'@
                $submitExpression = $submitExpressionTemplate.Replace('__RACE_ID__', $lastOkRace)
                $submitResp = Send-CdpCommand -Ws $ws -Id 20 -Method 'Runtime.evaluate' -Params @{
                    expression = $submitExpression
                    awaitPromise = $true
                    returnByValue = $true
                }

                $submitValue = $submitResp.result.result.value
                if ($submitValue -and $submitValue.status -eq 'ok') {
                    $submitFlow.submitStatus = 'clicked'
                    $submitFlow.submitMessage = [string]($submitValue.message | Out-String).Trim()

                    $receiptUrl = Wait-CdpLocation -Ws $ws -Pattern "bet_complete\.html.*race_id=$([regex]::Escape($lastOkRace))" -MaxWaitMs 12000 -PollMs 400 -StartId 21
                    if (-not $receiptUrl) {
                        $receiptUrl = Wait-CdpLocation -Ws $ws -Pattern 'bet_complete\.html' -MaxWaitMs 4000 -PollMs 400 -StartId 60
                    }

                    if ($receiptUrl) {
                        $submitFlow.submitStatus = 'ok'
                        $submitFlow.receiptUrl = $receiptUrl
                    } else {
                        $submitFlow.submitStatus = 'warn'
                        $submitFlow.submitMessage = 'Submit was clicked, but the receipt page was not confirmed before timeout.'
                    }

                    if ($goNextRequested) {
                        if ($nextRaceUrl) {
                            Start-Sleep -Milliseconds 900
                            [void](Send-CdpCommand -Ws $ws -Id 40 -Method 'Page.navigate' -Params @{ url = $nextRaceUrl })
                            $nextLanded = Wait-CdpLocation -Ws $ws -Pattern 'shutuba\.html.*race_id=' -MaxWaitMs 12000 -PollMs 400 -StartId 41
                            if ($nextLanded) {
                                $submitFlow.nextStatus = 'ok'
                                $submitFlow.nextLandingUrl = $nextLanded
                            } else {
                                $submitFlow.nextStatus = 'warn'
                                $submitFlow.nextMessage = 'Next-race navigation did not confirm before timeout.'
                            }
                        } else {
                            $submitFlow.nextStatus = 'warn'
                            $submitFlow.nextMessage = 'No next-race URL was found for the submitted race.'
                        }
                    }
                } else {
                    $submitFlow.submitStatus = 'error'
                    $submitFlow.submitMessage = if ($submitValue) { [string]($submitValue.message | Out-String).Trim() } else { 'Submit click did not return a usable result.' }
                    if ($goNextRequested) {
                        $submitFlow.nextStatus = 'skipped'
                    }
                }
            }
        } catch {
            $submitFlow.submitStatus = 'error'
            $submitFlow.submitMessage = $_.Exception.Message
            if ($goNextRequested -and $submitFlow.nextStatus -eq 'pending') {
                $submitFlow.nextStatus = 'skipped'
            }
        }

        if ($resultValue.PSObject.Properties.Name -contains 'submitFlow') {
            $resultValue.submitFlow = $submitFlow
        } else {
            $resultValue | Add-Member -NotePropertyName 'submitFlow' -NotePropertyValue $submitFlow
        }

        $messageSuffix = switch ($submitFlow.submitStatus) {
            'ok' { if ($submitFlow.nextStatus -eq 'ok') { ' Submitted and opened the next race page.' } else { ' Submitted the race in OrePro.' } }
            'clicked' { ' Submit was triggered in OrePro.' }
            'warn' { " Submit waited for the refreshed preview, but could not be safely confirmed. $($submitFlow.submitMessage)" }
            'error' { " Submit did not complete: $($submitFlow.submitMessage)" }
            default { '' }
        }
        $resultValue.message = ([string]($resultValue.message | Out-String)).Trim() + $messageSuffix
    }

    try { $ws.Dispose() } catch {}

    $resultValue | ConvertTo-Json -Depth 32 -Compress
    exit 0
} catch {
    Write-JsonAndExit -Code 1 -Status 'error' -Message ("Companion vote apply helper failed: " + $_.Exception.Message)
}
