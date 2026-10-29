"""Security tests against an isolated server on port 8767 with APP_PASSWORD set."""
import os, subprocess, sys, time, urllib.request, urllib.error, http.cookiejar, urllib.parse
SRC = "/Users/simonlevy/.gemini/antigravity/scratch/bnvt-dealflow"
env = dict(os.environ, PORT="8767", APP_PASSWORD="test-pass-123")
p = subprocess.Popen([sys.executable, "server.py"], cwd=SRC, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(2.5)
B = "http://localhost:8767"
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k): return None
plain = urllib.request.build_opener(NoRedirect)
def code(url, opener=plain, data=None):
    try:
        r = opener.open(urllib.request.Request(B + url, data=data), timeout=10); return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
results = []
def check(name, cond): results.append(cond); print(("PASS " if cond else "FAIL ") + name)
try:
    check("key file not served (404)", code("/db_storage/anthropic_api_key.json")[0] == 404)
    check("server.py not served (404)", code("/server.py")[0] == 404)
    check("app page redirects to login (302)", code("/")[0] == 302)
    check("API blocked without login (401)", code("/api/storage?key=bnvt-dealflow-v1")[0] == 401)
    check("healthz public (200)", code("/healthz")[0] == 200)
    check("wrong password rejected", b"Wrong password" in code("/login", data=b"password=nope")[1])
    jar = http.cookiejar.CookieJar()
    auth = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar), NoRedirect)
    st, _ = code("/login", auth, data=urllib.parse.urlencode({"password": "test-pass-123"}).encode())
    check("correct password sets session (302)", st == 302 and any(c.name == "bnvt_session" for c in jar))
    check("app page loads after login (200)", code("/", auth)[0] == 200)
    check("app JS loads after login (200)", code("/bnvt-dealflow.js", auth)[0] == 200)
    st, body = code("/api/storage?key=anthropic_api_key", auth)
    check("API key is masked", st == 200 and b"sk-ant" not in body and b"\\u2022" in body)
    st, body = code("/api/storage?key=bnvt-dealflow-v1", auth)
    check("deal data readable after login", st == 200 and len(body) > 1_000_000)
    check("path traversal key rejected", code("/api/storage?key=../server", auth)[0] == 400)
    st, body = code("/api/storage", auth, data=b'{"key":"anthropic_api_key","value":"\\u2022\\u2022\\u2022\\u2022abcd"}')
    check("masked value write ignored", b"unchanged" in body)
    tampered = urllib.request.build_opener(NoRedirect)
    tampered.addheaders = [("Cookie", "bnvt_session=9999999999.deadbeef")]
    check("forged cookie rejected", code("/api/storage?key=bnvt-dealflow-v1", tampered)[0] == 401)
finally:
    p.terminate(); p.wait()
print(f"\n{sum(results)}/{len(results)} passed")
