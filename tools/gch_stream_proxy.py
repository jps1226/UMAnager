"""
Standalone GreenChannel (GCh) live-stream metadata proxy for TV mode.

Why this exists: the JRA livestream (sp.gch.jp / streaks.jp) is geo-gated to Japan IPs.
Nexus itself must stay OUTSIDE the VPN tunnel (routing Nexus through Japan previously broke
OrePro/Discord connectivity live — see dev_log.md). This script is the one small piece that
DOES need a Japan-originating IP, so it runs as its own `python.exe` process and gets added to
ProtonVPN's split-tunnel INCLUDE list (Settings -> Split Tunneling -> Include mode -> check
python.exe), while Nexus.exe stays unchecked/unprotected.

Replicates GchController.cs's two-call logic exactly:
  1. GET https://sp.gch.jp/api/vij                              -> { project_id, id, api_key }
  2. GET https://playback.api.streaks.jp/v1/projects/<pid>/medias/<id>   (X-Streaks-Api-Key header)
Serves the combined { session, playback } JSON locally so Nexus can call this instead of
hitting JRA directly.

Run manually when you want TV mode:  python tools/gch_stream_proxy.py
Listens on 127.0.0.1:5057 only (not exposed to the LAN).
"""
import json
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = 5057
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def fetch_json(url, headers):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def build_playback_payload():
    vij_status, vij = fetch_json("https://sp.gch.jp/api/vij", {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://sp.gch.jp/jra",
    })
    if vij_status != 200:
        raise RuntimeError(f"gch /api/vij returned HTTP {vij_status}")

    project_id = vij.get("project_id", "")
    media_id = vij.get("id", "")
    api_key = vij.get("api_key", "")
    if not (project_id and media_id and api_key):
        raise RuntimeError("gch session response missing project_id/id/api_key")

    play_url = f"https://playback.api.streaks.jp/v1/projects/{project_id}/medias/{media_id}"
    play_status, playback = fetch_json(play_url, {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "X-Streaks-Api-Key": api_key,
        "Referer": "https://sp.gch.jp/",
        "Origin": "https://sp.gch.jp",
    })
    if play_status != 200:
        raise RuntimeError(f"streaks playback returned HTTP {play_status}")

    return {
        "session": {"project_id": project_id, "id": media_id},
        "playback": playback,
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print("[gch-proxy] " + (fmt % args))

    def do_GET(self):
        if self.path != "/live-playback-json":
            self.send_response(404)
            self.end_headers()
            return
        try:
            payload = build_playback_payload()
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as ex:
            body = json.dumps({"error": str(ex)}).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)


if __name__ == "__main__":
    server = HTTPServer(("127.0.0.1", PORT), Handler)
    print(f"[gch-proxy] listening on 127.0.0.1:{PORT} - Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
