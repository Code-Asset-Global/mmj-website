"""Mind, Machine & Justice - website + backend (Flask).
Production: Supabase Postgres (DATABASE_URL) + Supabase Storage (SUPABASE_URL, SUPABASE_SERVICE_KEY).
Local testing: if those are not set it falls back to SQLite + local ./local_uploads folder.
"""
import os, io, json, sqlite3, secrets, time, uuid, mimetypes, re
from datetime import datetime
from functools import wraps
from flask import (Flask, g, request, session, jsonify, send_from_directory, Response,
                   stream_with_context, redirect, render_template, abort)
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

BASE = os.path.dirname(os.path.abspath(__file__))
DATA, LOCAL = os.path.join(BASE, "data"), os.path.join(BASE, "local_uploads")
DATABASE_URL = os.getenv("DATABASE_URL")
PG = bool(DATABASE_URL)
SB_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SB_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
BUCKET = os.getenv("SUPABASE_BUCKET", "mmj-files")
SB = bool(SB_URL and SB_KEY)
ALLOWED = {"pdf", "doc", "docx", "ppt", "pptx", "xls", "xlsx", "txt", "csv", "zip", "mp4", "webm", "mov", "m4v", "mp3", "wav", "m4a",
           "png", "jpg", "jpeg", "gif", "webp"}
INLINE = {"pdf", "mp4", "webm", "mov", "m4v", "mp3", "wav", "m4a", "png", "jpg", "jpeg", "gif", "webp"}  # safe to show inside the page
MAX_MB = int(os.getenv("MAX_UPLOAD_MB", "50"))  # Supabase free plan allows 50 MB per file
IMG = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "webp": "image/webp"}
CATEGORIES = ["Brochure & Primer", "Session Materials", "Readings", "Assignments", "Recordings", "Other"]
SEED = [
    ("admin", "admin", "Course Admin", os.getenv("ADMIN_PASSWORD", "Admin@2026")),
    ("faculty", "faculty", "Law Faculty", os.getenv("FACULTY_PASSWORD", "Faculty@2026")),
    ("participant", "participant", "Participants", os.getenv("PARTICIPANT_PASSWORD", "Mmj@2026")),
    ("participant2", "participant", "Participants (backup)", os.getenv("PARTICIPANT2_PASSWORD", "Backup@2026")),
]

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = (MAX_MB + 2) * 1024 * 1024
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax",
                  SESSION_COOKIE_SECURE=os.getenv("HTTPS", "0") == "1", PERMANENT_SESSION_LIFETIME=43200)


# ---------- database (Postgres or SQLite behind one tiny wrapper) ----------
_VER = [0]      # bumped by every commit (except visit/log writes), invalidating the read cache below
_MEMO = {}


def memo(key, fn, ttl=30):
    """Cache a DB read in memory. Saves a network round trip to the remote database on each request."""
    h, t, v0 = _MEMO.get(key), time.time(), _VER[0]
    if h and h[0] == v0 and t - h[1] < ttl: return h[2]
    v = fn(); _MEMO[key] = (v0, t, v); return v


import threading
_tl = threading.local()  # one reusable Postgres connection per worker thread (opening a new one per request is slow)


def _pg_connect():
    import psycopg
    from psycopg.rows import dict_row
    c = getattr(_tl, "c", None)
    if c is None or c.closed:
        c = psycopg.connect(DATABASE_URL, row_factory=dict_row, prepare_threshold=None, connect_timeout=15)
        _tl.c = c
    return c


class Conn:
    def __init__(self):
        self.n = 0
        if PG:
            self.c = _pg_connect()
        else:
            os.makedirs(DATA, exist_ok=True)
            self.c = sqlite3.connect(os.path.join(DATA, "mmj.db"))
            self.c.row_factory = sqlite3.Row

    def execute(self, sql, args=()):
        first, self.n = self.n == 0, self.n + 1
        if not PG:
            return self.c.execute(sql, args)
        import psycopg
        sql = sql.replace("?", "%s")
        try:
            return self.c.execute(sql, args)
        except (psycopg.OperationalError, psycopg.InterfaceError):
            if not first: raise  # connection died mid-request: do not silently replay half a transaction
            try: self.c.close()
            except Exception: pass
            _tl.c = None; self.c = _pg_connect()  # idle connection was dropped by the server: reconnect once
            return self.c.execute(sql, args)

    def commit(self, bump=True):
        self.c.commit()
        if bump: _VER[0] += 1

    def close(self):
        if PG:
            try: self.c.rollback()  # keep the connection open for the next request
            except Exception: _tl.c = None
        else:
            self.c.close()


def db():
    if "db" not in g:
        g.db = Conn()
    return g.db


@app.teardown_appcontext
def close_db(_):
    d = g.pop("db", None)
    if d: d.close()


from datetime import timezone, timedelta
IST = timezone(timedelta(hours=5, minutes=30))
def now(): return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


# ---------- file storage (Supabase Storage or local folder) ----------
def _h(extra=None):
    return {"Authorization": "Bearer " + SB_KEY, "apikey": SB_KEY, **(extra or {})}


def store_put(name, data, ctype="application/octet-stream"):
    if SB:
        import requests
        r = requests.post("%s/storage/v1/object/%s/%s" % (SB_URL, BUCKET, name), data=data,
                          headers=_h({"Content-Type": ctype, "x-upsert": "true"}), timeout=300)
        if r.status_code >= 300: raise RuntimeError("Storage upload failed: " + r.text[:200])
    else:
        p = os.path.join(LOCAL, name); os.makedirs(os.path.dirname(p), exist_ok=True)
        open(p, "wb").write(data)


def store_get(name, rng=None):
    """returns (iterable_of_bytes, content_type, status, extra_headers). Supports HTTP Range so videos can seek."""
    if SB:
        import requests
        h = _h({"Range": rng} if rng else None)
        r = requests.get("%s/storage/v1/object/%s/%s" % (SB_URL, BUCKET, name), headers=h, stream=True, timeout=300)
        if r.status_code >= 300: abort(404)
        extra = {k: r.headers[k] for k in ("Content-Range", "Content-Length", "Accept-Ranges") if k in r.headers}
        return r.iter_content(65536), r.headers.get("Content-Type", "application/octet-stream"), r.status_code, extra
    p = os.path.join(LOCAL, name)
    if not os.path.exists(p): abort(404)
    ct = mimetypes.guess_type(p)[0] or "application/octet-stream"
    data = open(p, "rb").read(); total = len(data); extra = {"Accept-Ranges": "bytes"}
    m = re.match(r"bytes=(\d*)-(\d*)$", rng or "")
    if m and (m.group(1) or m.group(2)):
        a = int(m.group(1)) if m.group(1) else max(0, total - int(m.group(2)))
        b = min(int(m.group(2)), total - 1) if m.group(1) and m.group(2) else total - 1
        extra.update({"Content-Range": "bytes %d-%d/%d" % (a, b, total), "Content-Length": str(b - a + 1)})
        return iter([data[a:b + 1]]), ct, 206, extra
    extra["Content-Length"] = str(total)
    return iter([data]), ct, 200, extra


def store_del(name):
    try:
        if SB:
            import requests
            requests.delete("%s/storage/v1/object/%s/%s" % (SB_URL, BUCKET, name), headers=_h(), timeout=60)
        else:
            os.remove(os.path.join(LOCAL, name))
    except Exception:
        pass


# ---------- fast sign-in for a whole class logging in at the same second ----------
# scrypt is deliberately slow. A correct password is remembered (as a keyed hash, never the password itself) for 12 hours,
# so when 200 people use the shared ID together only the first one pays the cost and the rest are instant.
import hmac as _hmac, hashlib as _hashlib
_PWOK = {}; _PWKEY = secrets.token_bytes(16); _PWLOCKS = {}
def pw_ok(row, pw):
    k = (row["username"], row["pw_hash"], _hmac.new(_PWKEY, pw.encode(), _hashlib.sha256).hexdigest())
    if _PWOK.get(k, 0) > time.time(): return True
    with _PWLOCKS.setdefault(row["username"], threading.Lock()):   # others wait for the first check instead of all running scrypt
        if _PWOK.get(k, 0) > time.time(): return True
        if not check_password_hash(row["pw_hash"], pw): return False
        if len(_PWOK) > 500: _PWOK.clear()
        _PWOK[k] = time.time() + 43200; return True


# ---------- big files: let Supabase send them straight to the browser (not through this server) ----------
from urllib.parse import quote as _q
MEDIA_EXT = {"mp4", "webm", "mov", "m4v", "mp3", "wav", "m4a"}
_SIGN = {}
def store_url(name, filename=None, download=False):
    if not SB: return None
    k = (name, filename if download else None); hit = _SIGN.get(k)
    if hit and hit[1] > time.time(): return hit[0]
    try:
        import requests
        r = requests.post("%s/storage/v1/object/sign/%s/%s" % (SB_URL, BUCKET, name), json={"expiresIn": 7200},
                          headers=_h({"Content-Type": "application/json"}), timeout=15)
        if r.status_code >= 300: return None
        s = r.json().get("signedURL") or r.json().get("signedUrl") or ""
        if not s: return None
        u = s if s.startswith("http") else (SB_URL + s if s.startswith("/storage/v1") else SB_URL + "/storage/v1" + s)
        if download: u += ("&" if "?" in u else "?") + "download=" + _q(filename or "file")
        if len(_SIGN) > 1000: _SIGN.clear()
        _SIGN[k] = (u, time.time() + 6000); return u
    except Exception:
        return None

def direct(stored, orig, inline):
    """redirect to a short-lived Supabase link for videos/audio (playback) and for downloads; None = stream it ourselves"""
    ext = orig.rsplit(".", 1)[-1].lower() if orig and "." in orig else ""
    if inline and ext not in MEDIA_EXT: return None
    u = store_url(stored, orig, download=not inline)
    return redirect(u) if u else None


def init():
    c = Conn(); idt = "SERIAL PRIMARY KEY" if PG else "INTEGER PRIMARY KEY"
    for s in ["CREATE TABLE IF NOT EXISTS users(id %s, username TEXT UNIQUE, role TEXT, name TEXT, pw_hash TEXT, active INTEGER DEFAULT 1)" % idt,
              "CREATE TABLE IF NOT EXISTS files(id %s, title TEXT, category TEXT, session_no INTEGER, orig_name TEXT, stored TEXT, size BIGINT, uploaded_by TEXT, created TEXT)" % idt,
              "CREATE TABLE IF NOT EXISTS logs(id %s, ts TEXT, username TEXT, action TEXT, detail TEXT, ip TEXT)" % idt,
              "CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT)",
              "CREATE TABLE IF NOT EXISTS announcements(id %s, title TEXT, body TEXT, stored TEXT, orig_name TEXT, size BIGINT, posted_by TEXT, created TEXT)" % idt,
              "CREATE TABLE IF NOT EXISTS visits(id %s, day TEXT, vid TEXT, path TEXT, ts TEXT)" % idt,
              "CREATE TABLE IF NOT EXISTS attendance(id %s, day TEXT, pid INTEGER, status TEXT, via TEXT, marked_by TEXT, ts TEXT, UNIQUE(day,pid))" % idt,
              "CREATE TABLE IF NOT EXISTS law_entries(id %s, region TEXT, ltype TEXT, jurisdiction TEXT, ldate TEXT, title TEXT, summary TEXT, status TEXT, tags TEXT, caution TEXT, source TEXT, stored TEXT, orig_name TEXT, size BIGINT, added_by TEXT, created TEXT)" % idt,
              "CREATE TABLE IF NOT EXISTS participants(id %s, name TEXT, email TEXT, phone TEXT, org TEXT, ref TEXT, status TEXT, username TEXT, created TEXT, note TEXT)" % idt]:
        c.execute(s)
    if SB:
        try:
            import requests
            requests.post(SB_URL + "/storage/v1/bucket", json={"id": BUCKET, "name": BUCKET, "public": False}, headers=_h(), timeout=30)
        except Exception: pass
    if not c.execute("SELECT 1 AS x FROM users LIMIT 1").fetchone():
        for u, role, name, pw in SEED:
            c.execute("INSERT INTO users(username,role,name,pw_hash) VALUES(?,?,?,?)", (u, role, name, generate_password_hash(pw)))
        for fn, title in [("Mind_Machine_Justice_2026_Course_Brochure.pdf", "Course Brochure"),
                          ("Mind_Machine_Justice_2026_Participant_Primer.pdf", "Participant Primer")]:
            p = os.path.join(DATA, fn)
            if os.path.exists(p):
                data = open(p, "rb").read(); st = uuid.uuid4().hex + ".pdf"
                store_put(st, data, "application/pdf")
                c.execute("INSERT INTO files(title,category,orig_name,stored,size,uploaded_by,created) VALUES(?,?,?,?,?,?,?)",
                          (title, CATEGORIES[0], fn, st, len(data), "admin", now()))
    c.commit(); c.close()


def setting(k, default=None):
    v = memo(("s", k), lambda: (lambda r: r["v"] if r else None)(db().execute("SELECT v FROM settings WHERE k=?", (k,)).fetchone()))
    return v if v is not None else default


def jsetting(k, default):
    try: return json.loads(setting(k) or "")
    except Exception: return default


def set_jsetting(k, v): set_setting(k, json.dumps(v, ensure_ascii=False))


def set_setting(k, v):
    db().execute("DELETE FROM settings WHERE k=?", (k,))
    if v is not None: db().execute("INSERT INTO settings(k,v) VALUES(?,?)", (k, v))
    db().commit()


@app.context_processor
def inject():
    return {"brand": {"size": setting("logo_size", "70"), "v0": setting("logo0_v", "0"), "v1": setting("logo1_v", "0")}}


def log(action, detail="", user=None):
    db().execute("INSERT INTO logs(ts,username,action,detail,ip) VALUES(?,?,?,?,?)",
                 (now(), user or session.get("u", "-"), action, detail, request.remote_addr))
    db().commit(bump=False)


# ---------- auth ----------
IDLE_MIN = int(os.getenv("IDLE_MINUTES", "30"))   # signed out after this many minutes without using the portal
def me():
    if "u" not in session: return None
    t = time.time(); last = session.get("t", t)
    if t - last > IDLE_MIN * 60: session.clear(); return None
    if t - last > 30 or "t" not in session: session["t"] = t
    if session.get("d") != now()[:10]:  # sign-in is valid for the calendar day (IST) only; next day = sign in again
        session.clear(); return None
    r = memo(("me", session["u"]), lambda: (lambda x: dict(x) if x else None)(
        db().execute("SELECT username,role,name,active FROM users WHERE username=?", (session["u"],)).fetchone()))
    return r if r and r["active"] else None


def need(*roles):
    def deco(f):
        @wraps(f)
        def w(*a, **k):
            u = me()
            if not u: return jsonify(error="Please sign in"), 401
            if roles and u["role"] not in roles: return jsonify(error="Not allowed for your role"), 403
            if request.method in ("POST", "PUT", "PATCH", "DELETE") and request.headers.get("X-Requested-With") != "fetch":
                return jsonify(error="Bad request"), 400
            return f(*a, **k)
        return w
    return deco


FAILS = {}
def throttled(ip, limit=8):
    t = time.time(); FAILS[ip] = [x for x in FAILS.get(ip, []) if t - x < 600]
    return len(FAILS[ip]) >= limit


def client_ip():
    return (request.headers.get("X-Forwarded-For", request.remote_addr) or "").split(",")[0].strip()


# ---------- speed: gzip text responses, short browser cache for static files ----------
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 300
_GZ = ("text/html", "text/css", "text/javascript", "application/javascript", "application/json", "image/svg+xml")


@app.after_request
def speed(resp):
    try:
        if (resp.status_code == 200 and "gzip" in request.headers.get("Accept-Encoding", "")
                and "Content-Encoding" not in resp.headers and resp.mimetype in _GZ):
            resp.direct_passthrough = False
            data = resp.get_data()
            if len(data) > 600:
                import gzip
                resp.set_data(gzip.compress(data, 5))
                resp.headers["Content-Encoding"] = "gzip"
                resp.headers.add("Vary", "Accept-Encoding")
    except Exception:
        pass
    return resp


# ---------- pages ----------
import queue
_visitq, _SEEN, _vw = queue.Queue(), set(), []


def _visit_worker():
    while True:
        day, vid, path = _visitq.get()
        try:
            c = Conn()
            if not c.execute("SELECT 1 AS x FROM visits WHERE day=? AND vid=? AND path=?", (day, vid, path)).fetchone():
                c.execute("INSERT INTO visits(day,vid,path,ts) VALUES(?,?,?,?)", (day, vid, path, now())); c.commit(bump=False)
            c.close()
        except Exception: pass


def track(path):
    """Count a visit in the background so the page does not wait for the database."""
    try:
        vid = __import__("hashlib").sha256((client_ip() + request.headers.get("User-Agent", "")).encode()).hexdigest()[:16]
        key = (now()[:10], vid, path)
        if key in _SEEN: return
        if len(_SEEN) > 50000: _SEEN.clear()
        _SEEN.add(key)
        if not _vw:
            _vw.append(threading.Thread(target=_visit_worker, daemon=True, name="visits")); _vw[0].start()
        _visitq.put(key)
    except Exception: pass


def boot_json(u):
    """Everything the portal needs on first paint, embedded in the HTML so no extra API calls are needed."""
    vf = app.view_functions
    def get(name):
        try:
            r = vf[name](); r = r[0] if isinstance(r, tuple) else r
            return r.get_json() if getattr(r, "status_code", 200) == 200 else None
        except Exception: return None
    b = {"me": get("api_me"), "content": get("api_content"), "foundation": get("api_foundation"), "settings": get("api_settings"),
         "files": get("api_files"), "ann": get("ann_list"), "custom": get("laws_list"),
         "feedback": get("fb_get"), "daynotes": get("dn_list")}
    if u["role"] == "admin": b["part"] = get("a_participants")
    return json.dumps(b, ensure_ascii=False).replace("<", "\\u003c")


@app.route("/")
def home():
    track("/")
    u = me()
    if not u: return redirect("/login")
    return render_template("portal.html", role=u["role"], boot=boot_json(u))

@app.route("/healthz")
def healthz(): return "ok"

@app.route("/login")
def login_page():
    track("/login")
    u = me()
    if u: return redirect("/")
    return render_template("login.html", contact=course()["course"]["contact"])

# old addresses keep working
@app.route("/admin")
@app.route("/faculty")
@app.route("/console")
def staff_page(): return redirect("/")

@app.route("/library")
def library_page(): return redirect("/#library")


@app.get("/logo/<int:n>")
def logo(n):
    if n not in (0, 1): abort(404)
    name = setting("logo%d" % n)
    if name:
        it, ct, _, _ = store_get(name)
        return Response(it, mimetype=ct, headers={"Cache-Control": "public, max-age=86400"})
    sd = os.path.join(BASE, "static")
    fn = "logo%d.png" % n if os.path.exists(os.path.join(sd, "logo%d.png" % n)) else "logo%d.jpg" % n  # PNG (lossless) preferred
    return send_from_directory(sd, fn, max_age=86400)


@app.errorhandler(413)
def too_big(_): return jsonify(error="File is larger than %d MB. For long videos, add a link instead." % MAX_MB), 413


# ---------- auth API ----------
@app.post("/api/login")
def api_login():
    ip = client_ip(); d = request.get_json(silent=True) or {}
    # limit is per (network + ID), so one wrong password on a shared campus Wi-Fi cannot lock the whole room out
    tkey = ip + "|" + str(d.get("username", "")).strip().lower()[:60]
    if throttled(tkey, 25): return jsonify(error="Too many attempts. Try again in 10 minutes."), 429
    r = db().execute("SELECT * FROM users WHERE username=?", (str(d.get("username", "")).strip().lower(),)).fetchone()
    if not r or not r["active"] or not pw_ok(r, str(d.get("password", ""))):
        FAILS.setdefault(tkey, []).append(time.time())
        log("login_failed", str(d.get("username", ""))[:40], user="-")
        return jsonify(error="Wrong ID or password"), 401
    session.clear(); session.permanent = False; session["t"] = time.time(); session["u"] = r["username"]; session["d"] = now()[:10]; log("login")
    return jsonify(ok=True, redirect="/")

@app.post("/api/logout")
def api_logout(): session.clear(); return jsonify(ok=True)

@app.get("/api/me")
def api_me():
    u = me(); return jsonify(user=dict(u) if u else None, categories=CATEGORIES, max_mb=MAX_MB)


# ---------- content ----------
_FILE_CACHE = {}


def read_json(name):
    p = os.path.join(DATA, name); m = os.path.getmtime(p); hit = _FILE_CACHE.get(name)
    if not hit or hit[0] != m:
        with open(p, encoding="utf-8") as f: hit = _FILE_CACHE[name] = (m, f.read())
    return json.loads(hit[1])


def course(): return read_json("course.json")

@app.get("/api/public")
def api_public():
    c = course(); keep = ("title", "subtitle", "edition", "dates", "hours", "fee", "contact", "timezone")
    return jsonify(course={k: c["course"][k] for k in keep},
                   sessions=[{k: s[k] for k in ("n", "date", "start", "end", "title", "theme")} for s in c["sessions"]])

@app.get("/api/content")
@need()
def api_content():
    c = course(); c["course"].pop("payment", None); c["course"].pop("registration", None); return jsonify(c)


@app.get("/api/foundation")
@need()
def api_foundation():
    return jsonify(read_json("foundation.json"))

@app.get("/api/settings")
@need()
def api_settings():
    return jsonify(overrides=jsetting("overrides", {}), links=jsetting("links", {}), formUrl=setting("form_url", ""),
                   assets={k: bool(find_asset(k)) for k in ASSET_FILES})

ASSET_FILES = {"brochure": "Mind_Machine_Justice_2026_Course_Brochure.pdf", "primer": "Mind_Machine_Justice_2026_Participant_Primer.pdf"}
ASSET_WORDS = {"brochure": "brochure", "primer": "primer"}


def find_asset(key):
    """Locate the brochure/primer PDF. Order: exact name in data/, any PDF in data/ whose name has the keyword,
    then the newest matching file uploaded from 'Upload & files' (stored in Supabase). Returns (kind, value, filename) or None."""
    word = ASSET_WORDS[key]
    exact = os.path.join(DATA, ASSET_FILES[key])
    if os.path.isfile(exact): return "disk", exact, ASSET_FILES[key]
    if os.path.isdir(DATA):
        for fn in sorted(os.listdir(DATA)):
            if fn.lower().endswith(".pdf") and word in fn.lower(): return "disk", os.path.join(DATA, fn), fn
    r = db().execute("SELECT stored,orig_name FROM files WHERE (LOWER(title) LIKE ? OR LOWER(orig_name) LIKE ?) AND LOWER(orig_name) LIKE ? "
                     "ORDER BY id DESC LIMIT 1", ("%" + word + "%", "%" + word + "%", "%.pdf")).fetchone()
    if r: return "store", r["stored"], r["orig_name"]
    return None


@app.get("/api/asset/<key>")
@need()
def api_asset(key):
    if key == "cover":
        p = os.path.join(BASE, "static", "brochure-cover.png")
        if os.path.isfile(p): return send_from_directory(os.path.join(BASE, "static"), "brochure-cover.png")
        r = db().execute("SELECT stored,orig_name FROM files WHERE LOWER(title) LIKE ? ORDER BY id DESC LIMIT 1", ("%cover%",)).fetchone()
        if not r: abort(404)
        it, ct, _, _ = store_get(r["stored"])
        return Response(it, mimetype=mimetypes.guess_type(r["orig_name"])[0] or ct)
    if key not in ASSET_FILES: abort(404)
    hit = find_asset(key)
    if not hit:
        label = "course brochure" if key == "brochure" else "participant primer"
        return jsonify(error="The %s PDF has not been added to the portal yet. Admin: open Upload & files, choose the PDF, "
                             "set the title to '%s' and upload it. It will then download from here." % (label, label.title())), 404
    log("download", hit[2])
    if hit[0] == "disk":
        return send_from_directory(os.path.dirname(hit[1]), os.path.basename(hit[1]), as_attachment=True, download_name=hit[2],
                                   mimetype="application/pdf")
    it, _, _, extra = store_get(hit[1])
    return Response(stream_with_context(it), mimetype="application/pdf", headers={
        "Content-Disposition": 'attachment; filename="%s"' % hit[2], "X-Content-Type-Options": "nosniff",
        **({"Content-Length": extra["Content-Length"]} if "Content-Length" in extra else {})})


# ---------- files ----------
@app.get("/api/files")
@need()
def api_files():
    return jsonify(memo("files", lambda: [dict(r) for r in db().execute("SELECT id,title,category,session_no,orig_name,size,uploaded_by,created FROM files ORDER BY id DESC").fetchall()]))

@app.post("/api/files")
@need("admin", "faculty")
def api_upload():
    f = request.files.get("file"); title = (request.form.get("title") or "").strip()
    cat = request.form.get("category", "Other")
    if not f or not f.filename: return jsonify(error="Choose a file"), 400
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED: return jsonify(error="File type .%s not allowed" % ext), 400
    if cat not in CATEGORIES: cat = "Other"
    if cat == "Other" and re.search(r"brochure|primer", (title + " " + f.filename).lower()): cat = CATEGORIES[0]  # brochure / primer always land in their own category
    sn = request.form.get("session_no"); sn = int(sn) if sn and sn.isdigit() and 1 <= int(sn) <= 14 else None
    data = f.read(); stored = uuid.uuid4().hex + "." + ext
    try: store_put(stored, data, f.mimetype or "application/octet-stream")
    except Exception as e: return jsonify(error=str(e)), 502
    orig = secure_filename(f.filename) or ("file." + ext)
    db().execute("INSERT INTO files(title,category,session_no,orig_name,stored,size,uploaded_by,created) VALUES(?,?,?,?,?,?,?,?)",
                 (title or orig, cat, sn, orig, stored, len(data), session["u"], now())); db().commit()
    log("upload", title or orig); return jsonify(ok=True)

@app.get("/api/files/<int:fid>/download")
@need()
def api_download(fid):
    r = db().execute("SELECT * FROM files WHERE id=?", (fid,)).fetchone()
    if not r: abort(404)
    log("download", r["title"]); d = direct(r["stored"], r["orig_name"], False)
    if d: return d
    it, ct, _, extra = store_get(r["stored"])
    return Response(stream_with_context(it), mimetype=ct, headers={
        "Content-Disposition": 'attachment; filename="%s"' % r["orig_name"], "X-Content-Type-Options": "nosniff",
        **({"Content-Length": extra["Content-Length"]} if "Content-Length" in extra else {})})

@app.get("/api/files/<int:fid>/view")
@need()
def api_view(fid):
    """inline playback / preview (videos, audio, images, PDF). Supports Range so videos can seek."""
    r = db().execute("SELECT * FROM files WHERE id=?", (fid,)).fetchone()
    if not r: abort(404)
    ext = r["orig_name"].rsplit(".", 1)[-1].lower() if "." in r["orig_name"] else ""
    if ext not in INLINE: return redirect("/api/files/%d/download" % fid)
    d = direct(r["stored"], r["orig_name"], True)
    if d: return d
    it, _, status, extra = store_get(r["stored"], request.headers.get("Range"))
    ct = mimetypes.guess_type(r["orig_name"])[0] or "application/octet-stream"
    return Response(stream_with_context(it), status=status, mimetype=ct, headers={
        "Content-Disposition": 'inline; filename="%s"' % r["orig_name"], "X-Content-Type-Options": "nosniff",
        "Accept-Ranges": "bytes", **extra})

@app.delete("/api/files/<int:fid>")
@need("admin", "faculty")
def api_delete(fid):
    u = me(); r = db().execute("SELECT * FROM files WHERE id=?", (fid,)).fetchone()
    if not r: abort(404)
    if u["role"] == "faculty" and r["uploaded_by"] != u["username"]:
        return jsonify(error="Faculty can delete only their own uploads"), 403
    store_del(r["stored"]); db().execute("DELETE FROM files WHERE id=?", (fid,)); db().commit(); log("delete", r["title"])
    return jsonify(ok=True)


# ---------- participants: sign-up requests + registry (admin) ----------
SIGNUPS = {}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def make_login(name, email):
    base = re.sub(r"[^a-z0-9]", "", (email.split("@")[0] or name).lower())[:16] or "user"
    u, i = base, 1
    while db().execute("SELECT 1 AS x FROM users WHERE username=?", (u,)).fetchone():
        i += 1; u = base + str(i)
    pw = "".join(secrets.choice("abcdefghjkmnpqrstuvwxyz23456789") for _ in range(10))
    db().execute("INSERT INTO users(username,role,name,pw_hash) VALUES(?,?,?,?)", (u, "participant", name, generate_password_hash(pw)))
    db().commit(); return u, pw

def clean(d, k, n): return str(d.get(k, "") or "").strip()[:n]

@app.post("/api/signup")
def api_signup():
    return jsonify(error="Sign-up is closed. Please contact the course team."), 403

def _signup_disabled():
    if request.headers.get("X-Requested-With") != "fetch": return jsonify(error="Bad request"), 400
    ip = client_ip(); t = time.time(); SIGNUPS[ip] = [x for x in SIGNUPS.get(ip, []) if t - x < 3600]
    if len(SIGNUPS[ip]) >= 5: return jsonify(error="Too many requests. Please try again later."), 429
    d = request.get_json(silent=True) or {}
    name, email, phone, org, ref = clean(d, "name", 120), clean(d, "email", 160).lower(), clean(d, "phone", 30), clean(d, "org", 160), clean(d, "ref", 120)
    if not name or not ref or not EMAIL_RE.match(email): return jsonify(error="Please fill name, a valid email and the payment reference"), 400
    SIGNUPS[ip].append(t)
    if not db().execute("SELECT 1 AS x FROM participants WHERE email=? AND status='pending'", (email,)).fetchone():
        db().execute("INSERT INTO participants(name,email,phone,org,ref,status,created) VALUES(?,?,?,?,?,?,?)",
                     (name, email, phone, org, ref, "pending", now())); db().commit()
        log("signup_request", email, user="-")
    return jsonify(ok=True)

@app.get("/api/admin/participants")
@need("admin")
def a_participants():
    rows = [dict(r) for r in db().execute("SELECT * FROM participants ORDER BY id DESC").fetchall()]
    st = {}
    for r in db().execute("SELECT username, action, COUNT(*) AS n, MAX(ts) AS last FROM logs WHERE action IN ('login','download') GROUP BY username, action").fetchall():
        st.setdefault(r["username"], {})[r["action"]] = (r["n"], r["last"])
    for p in rows:
        s = st.get(p["username"] or "", {})
        p["logins"] = s.get("login", (0, ""))[0]; p["last_login"] = s.get("login", (0, ""))[1] or ""
        p["downloads"] = s.get("download", (0, ""))[0]
    return jsonify(rows)

@app.post("/api/admin/participants")
@need("admin")
def a_participant_add():
    d = request.get_json(silent=True) or {}
    name, email, phone, org, ref = clean(d, "name", 120), clean(d, "email", 160).lower(), clean(d, "phone", 30), clean(d, "org", 160), clean(d, "ref", 120)
    if not name or not EMAIL_RE.match(email): return jsonify(error="Name and a valid email are required"), 400
    u, pw = make_login(name, email)
    db().execute("INSERT INTO participants(name,email,phone,org,ref,status,username,created) VALUES(?,?,?,?,?,?,?,?)",
                 (name, email, phone, org, ref, "approved", u, now())); db().commit()
    log("participant_added", email); return jsonify(ok=True, username=u, password=pw)

def _part(pid):
    r = db().execute("SELECT * FROM participants WHERE id=?", (pid,)).fetchone()
    if not r: abort(404)
    return r

@app.post("/api/admin/participants/<int:pid>/approve")
@need("admin")
def a_participant_approve(pid):
    p = _part(pid)
    if p["username"]: return jsonify(error="Already approved"), 400
    u, pw = make_login(p["name"], p["email"])
    db().execute("UPDATE participants SET status='approved', username=? WHERE id=?", (u, pid)); db().commit()
    log("participant_approved", p["email"]); return jsonify(ok=True, username=u, password=pw)

@app.post("/api/admin/participants/<int:pid>/reset")
@need("admin")
def a_participant_reset(pid):
    p = _part(pid)
    if not p["username"]: return jsonify(error="No login yet"), 400
    pw = "".join(secrets.choice("abcdefghjkmnpqrstuvwxyz23456789") for _ in range(10))
    db().execute("UPDATE users SET pw_hash=?, active=1 WHERE username=?", (generate_password_hash(pw), p["username"])); db().commit()
    log("password_changed", p["username"]); return jsonify(ok=True, username=p["username"], password=pw)

@app.post("/api/admin/participants/<int:pid>/reject")
@need("admin")
def a_participant_reject(pid):
    p = _part(pid)
    if p["username"]: db().execute("UPDATE users SET active=0 WHERE username=?", (p["username"],))
    db().execute("UPDATE participants SET status='rejected' WHERE id=?", (pid,)); db().commit()
    log("participant_rejected", p["email"]); return jsonify(ok=True)

@app.delete("/api/admin/participants/<int:pid>")
@need("admin")
def a_participant_del(pid):
    p = _part(pid)
    if p["username"]: db().execute("DELETE FROM users WHERE username=? AND role='participant'", (p["username"],))
    db().execute("DELETE FROM attendance WHERE pid=?", (pid,))   # also remove this person's attendance records
    db().execute("DELETE FROM participants WHERE id=?", (pid,)); db().commit()
    log("participant_deleted", p["email"]); return jsonify(ok=True)


# ---------- announcements / posters (admin + faculty post and delete, everyone reads) ----------
@app.get("/api/announcements")
@need()
def ann_list():
    return jsonify(memo("ann", lambda: [dict(r) for r in db().execute("SELECT id,title,body,orig_name,size,posted_by,created FROM announcements ORDER BY id DESC LIMIT 100").fetchall()]))

@app.post("/api/announcements")
@need("admin", "faculty")
def ann_add():
    title = (request.form.get("title") or "").strip()[:160]; body = (request.form.get("body") or "").strip()[:3000]
    if not title: return jsonify(error="Please add a title"), 400
    f = request.files.get("file"); stored = orig = None; size = 0
    if f and f.filename:
        ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
        if ext not in ALLOWED: return jsonify(error="File type .%s not allowed" % ext), 400
        data = f.read(); stored = "announcements/" + uuid.uuid4().hex + "." + ext
        try: store_put(stored, data, f.mimetype or "application/octet-stream")
        except Exception as e: return jsonify(error=str(e)), 502
        orig = secure_filename(f.filename) or ("file." + ext); size = len(data)
    db().execute("INSERT INTO announcements(title,body,stored,orig_name,size,posted_by,created) VALUES(?,?,?,?,?,?,?)",
                 (title, body, stored, orig, size, session["u"], now())); db().commit()
    log("announcement_posted", title); return jsonify(ok=True)

@app.get("/api/announcements/<int:aid>/file")
@need()
def ann_file(aid):
    r = db().execute("SELECT * FROM announcements WHERE id=?", (aid,)).fetchone()
    if not r or not r["stored"]: abort(404)
    ext = r["orig_name"].rsplit(".", 1)[-1].lower() if "." in r["orig_name"] else ""
    inline = ext in INLINE and not request.args.get("dl")
    d = direct(r["stored"], r["orig_name"], inline)
    if d: return d
    it, _, status, extra = store_get(r["stored"], request.headers.get("Range"))
    ct = mimetypes.guess_type(r["orig_name"])[0] or "application/octet-stream"
    return Response(stream_with_context(it), status=status, mimetype=ct, headers={
        "Content-Disposition": '%s; filename="%s"' % ("inline" if inline else "attachment", r["orig_name"]),
        "X-Content-Type-Options": "nosniff", "Accept-Ranges": "bytes", **extra})

@app.delete("/api/announcements/<int:aid>")
@need("admin", "faculty")
def ann_del(aid):
    u = me(); r = db().execute("SELECT * FROM announcements WHERE id=?", (aid,)).fetchone()
    if not r: abort(404)
    if u["role"] == "faculty" and r["posted_by"] != u["username"]:
        return jsonify(error="Faculty can delete only their own announcements"), 403
    if r["stored"]: store_del(r["stored"])
    db().execute("DELETE FROM announcements WHERE id=?", (aid,)); db().commit(); log("announcement_deleted", r["title"])
    return jsonify(ok=True)


# ---------- assignments: published by admin / faculty, visible to everyone ----------
@app.get("/api/assignments")
@need()
def asg_list():
    # read straight from the database (no cache) so every participant always sees the latest published form link
    r = db().execute("SELECT v FROM settings WHERE k='assignments'").fetchone()
    try: return jsonify(json.loads(r["v"]) if r else [])
    except Exception: return jsonify([])

@app.post("/api/assignments")
@need("admin", "faculty")
def asg_save():
    d = request.get_json(silent=True) or {}
    title = clean(d, "title", 160); ins = clean(d, "instructions", 4000); day = clean(d, "day", 40) or "Course-wide"
    deadline = clean(d, "deadline", 40); url = str(d.get("form_url") or "").strip()
    if not title: return jsonify(error="Please add a title"), 400
    if url and not https_or_none(url): return jsonify(error="Please paste a valid https:// Google Form link"), 400
    L = jsetting("assignments", []); aid = d.get("id")
    if aid:
        it = next((x for x in L if x.get("id") == aid), None)
        if not it: abort(404)
        it.update(title=title, instructions=ins, day=day, deadline=deadline, form_url=url, updated=now())
        log("assignment_edited", title)
    else:
        L.insert(0, {"id": int(time.time() * 1000), "title": title, "instructions": ins, "day": day, "deadline": deadline,
                     "form_url": url, "posted_by": session["u"], "created": now()})
        log("assignment_published", title)
    set_jsetting("assignments", L[:200]); return jsonify(ok=True)

@app.delete("/api/assignments/<int:aid>")
@need("admin", "faculty")
def asg_del(aid):
    L = jsetting("assignments", []); it = next((x for x in L if x.get("id") == aid), None)
    if not it: abort(404)
    set_jsetting("assignments", [x for x in L if x.get("id") != aid]); log("assignment_deleted", it.get("title", "")); return jsonify(ok=True)


# ---------- feedback form + day-by-day initiation notes (admin / faculty write, everyone reads) ----------
def fresh_json(k, default):
    """read straight from the database (no cache) so every participant sees the latest version at once"""
    r = db().execute("SELECT v FROM settings WHERE k=?", (k,)).fetchone()
    try: return json.loads(r["v"]) if r else default
    except Exception: return default

@app.get("/api/feedback")
@need()
def fb_get(): return jsonify(fresh_json("feedback", {}))

@app.post("/api/feedback")
@need("admin", "faculty")
def fb_save():
    d = request.get_json(silent=True) or {}; url = str(d.get("url") or "").strip()
    if not url:
        set_setting("feedback", None); log("feedback_form_removed"); return jsonify(ok=True)
    if not https_or_none(url): return jsonify(error="Please paste a valid https:// Google Form link"), 400
    set_jsetting("feedback", {"url": url, "title": clean(d, "title", 160) or "Course feedback form", "note": str(d.get("note") or "").strip()[:1500],
                              "updated": now(), "by": session["u"]})
    log("feedback_form_saved"); return jsonify(ok=True)

@app.get("/api/daynotes")
@need()
def dn_list(): return jsonify(fresh_json("daynotes", []))

@app.post("/api/daynotes")
@need("admin", "faculty")
def dn_save():
    d = request.get_json(silent=True) or {}
    try: day = int(d.get("day"))
    except Exception: day = 0
    if not 1 <= day <= 7: return jsonify(error="Choose a day between 1 and 7"), 400
    body = str(d.get("body") or "").strip()[:8000]
    if not body: return jsonify(error="Please write the message"), 400
    L = [x for x in fresh_json("daynotes", []) if x.get("day") != day]
    L.append({"day": day, "title": clean(d, "title", 160), "body": body, "updated": now(), "by": session["u"]})
    L.sort(key=lambda x: x["day"]); set_jsetting("daynotes", L); log("day_note_saved", "Day %d" % day); return jsonify(ok=True)

@app.delete("/api/daynotes/<int:day>")
@need("admin", "faculty")
def dn_del(day):
    L = fresh_json("daynotes", [])
    if not any(x.get("day") == day for x in L): abort(404)
    set_jsetting("daynotes", [x for x in L if x.get("day") != day]); log("day_note_deleted", "Day %d" % day); return jsonify(ok=True)


# ---------- law tracker: entries and documents added by admin / faculty ----------
LAW_REGIONS = ("Global", "India"); LAW_TYPES = ("Legislation", "Judicial decisions", "Policy frameworks")

def law_json(r):
    return {"id": "custom-%d" % r["id"], "cid": r["id"], "custom": True, "region": r["region"], "type": r["ltype"],
            "jurisdiction": r["jurisdiction"] or r["region"], "date": r["ldate"] or str(r["created"])[:10], "title": r["title"],
            "summary": r["summary"] or "", "status": r["status"] or "Added by the course team",
            "tags": [t.strip() for t in (r["tags"] or "").split(",") if t.strip()][:8], "caution": r["caution"] or "Added by the course team. Check the primary source before relying on it.",
            "source": r["source"] or "", "sourceLabel": r["source"] or (r["orig_name"] or "Uploaded document"),
            "checked": str(r["created"])[:10], "fileName": r["orig_name"] or "", "fileSize": r["size"] or 0, "addedBy": r["added_by"]}

@app.get("/api/laws/custom")
@need()
def laws_list():
    return jsonify(memo("laws", lambda: [law_json(r) for r in db().execute("SELECT * FROM law_entries ORDER BY id DESC LIMIT 500").fetchall()]))

@app.post("/api/laws/custom")
@need("admin", "faculty")
def laws_add():
    f = request.form
    title = (f.get("title") or "").strip()[:240]
    if not title: return jsonify(error="Please add a title"), 400
    region = f.get("region") if f.get("region") in LAW_REGIONS else "Global"
    ltype = f.get("type") if f.get("type") in LAW_TYPES else "Legislation"
    ldate = f.get("date") if re.match(r"^\d{4}-\d{2}-\d{2}$", f.get("date") or "") else now()[:10]
    src = (f.get("source") or "").strip()
    if src and not https_or_none(src): return jsonify(error="The source link must start with https://"), 400
    stored = orig = None; size = 0; up = request.files.get("file")
    if up and up.filename:
        ext = up.filename.rsplit(".", 1)[-1].lower() if "." in up.filename else ""
        if ext not in ALLOWED: return jsonify(error="File type .%s not allowed" % ext), 400
        data = up.read(); stored = "laws/" + uuid.uuid4().hex + "." + ext
        try: store_put(stored, data, up.mimetype or "application/octet-stream")
        except Exception as e: return jsonify(error=str(e)), 502
        orig = secure_filename(up.filename) or ("file." + ext); size = len(data)
    db().execute("INSERT INTO law_entries(region,ltype,jurisdiction,ldate,title,summary,status,tags,caution,source,stored,orig_name,size,added_by,created) "
                 "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (region, ltype, (f.get("jurisdiction") or "").strip()[:80], ldate, title, (f.get("summary") or "").strip()[:2000],
                  (f.get("status") or "").strip()[:120], (f.get("tags") or "").strip()[:200], (f.get("caution") or "").strip()[:1000],
                  src, stored, orig, size, session["u"], now())); db().commit()
    log("law_added", title); return jsonify(ok=True)

@app.get("/api/laws/custom/<int:lid>/file")
@need()
def laws_file(lid):
    r = db().execute("SELECT * FROM law_entries WHERE id=?", (lid,)).fetchone()
    if not r or not r["stored"]: abort(404)
    ext = r["orig_name"].rsplit(".", 1)[-1].lower() if "." in r["orig_name"] else ""
    inline = ext in INLINE and not request.args.get("dl")
    d = direct(r["stored"], r["orig_name"], inline)
    if d: return d
    it, _, status, extra = store_get(r["stored"], request.headers.get("Range"))
    ct = mimetypes.guess_type(r["orig_name"])[0] or "application/octet-stream"
    log("download", r["title"])
    return Response(stream_with_context(it), status=status, mimetype=ct, headers={
        "Content-Disposition": '%s; filename="%s"' % ("inline" if inline else "attachment", r["orig_name"]),
        "X-Content-Type-Options": "nosniff", "Accept-Ranges": "bytes", **extra})

@app.delete("/api/laws/custom/<int:lid>")
@need("admin", "faculty")
def laws_del(lid):
    u = me(); r = db().execute("SELECT * FROM law_entries WHERE id=?", (lid,)).fetchone()
    if not r: abort(404)
    if u["role"] == "faculty" and r["added_by"] != u["username"]:
        return jsonify(error="Faculty can delete only their own entries"), 403
    if r["stored"]: store_del(r["stored"])
    db().execute("DELETE FROM law_entries WHERE id=?", (lid,)); db().commit(); log("law_deleted", r["title"])
    return jsonify(ok=True)


# ---------- admin overview, visitors ----------
def cnt(sql, a=()): return db().execute(sql, a).fetchone()["n"]

def att_rate():
    days = cnt("SELECT COUNT(DISTINCT day) AS n FROM attendance"); ppl = cnt("SELECT COUNT(*) AS n FROM participants WHERE status='approved'")
    pres = cnt("SELECT COUNT(*) AS n FROM attendance WHERE status='present'")
    return round(100.0 * pres / (days * ppl), 1) if days and ppl else 0

@app.get("/api/admin/overview")
@need("admin")
def a_overview():
    today = now()[:10]
    days = [r["day"] for r in db().execute("SELECT DISTINCT day FROM visits ORDER BY day DESC LIMIT 14").fetchall()][::-1]
    series = [{"day": d, "visitors": cnt("SELECT COUNT(DISTINCT vid) AS n FROM visits WHERE day=?", (d,))} for d in days]
    return jsonify(
        visitors_today=cnt("SELECT COUNT(DISTINCT vid) AS n FROM visits WHERE day=?", (today,)),
        visitors_total=cnt("SELECT COUNT(DISTINCT vid) AS n FROM visits"),
        pageviews=cnt("SELECT COUNT(*) AS n FROM visits"),
        signups=cnt("SELECT COUNT(*) AS n FROM participants"),
        pending=cnt("SELECT COUNT(*) AS n FROM participants WHERE status='pending'"),
        approved=cnt("SELECT COUNT(*) AS n FROM participants WHERE status='approved'"),
        logins_today=cnt("SELECT COUNT(*) AS n FROM logs WHERE action='login' AND ts LIKE ?", (today + "%",)),
        present_today=cnt("SELECT COUNT(*) AS n FROM attendance WHERE day=? AND status='present'", (today,)),
        registered=cnt("SELECT COUNT(*) AS n FROM users WHERE role='participant' AND active=1"),
        students_today=cnt("SELECT COUNT(DISTINCT username) AS n FROM logs WHERE action='login' AND ts LIKE ? AND username IN (SELECT username FROM users WHERE role='participant')", (today + "%",)),
        today_logins=[dict(r) for r in db().execute("SELECT l.username AS username, u.name AS name, u.role AS role, MIN(l.ts) AS first_ts, COUNT(*) AS n FROM logs l LEFT JOIN users u ON u.username=l.username WHERE l.action='login' AND l.ts LIKE ? GROUP BY l.username, u.name, u.role ORDER BY first_ts DESC", (today + "%",)).fetchall()],
        attendance_rate=att_rate(),
        series=series)

# ---------- attendance (faculty + admin) ----------
def valid_day(d):
    return d if re.match(r"^\d{4}-\d{2}-\d{2}$", d or "") else now()[:10]

def day_no(day):
    """Course day number (Day 1 = date of the first session in course.json), or None."""
    try:
        first = min(x["date"] for x in course()["sessions"])
        n = (datetime.strptime(day, "%Y-%m-%d") - datetime.strptime(first, "%Y-%m-%d")).days + 1
        return n if n >= 1 else None
    except Exception: return None


def day_label(day):
    n = day_no(day)
    return ("Day %d \u00b7 %s" % (n, day)) if n else day


def day_codes():
    """{day: (code, is_open)} for every day that has a check-in code (closed days keep their code)."""
    out = {}
    for r in db().execute("SELECT k,v FROM settings WHERE k LIKE ?", ("checkin_20%",)).fetchall():
        d = r["k"][len("checkin_"):]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", d) and r["v"]:
            out[d] = (r["v"], setting("checkin_state_" + d) != "closed")
    return out


def fresh_code(taken):
    for _ in range(200):
        c = "%04d" % secrets.randbelow(10000)
        if c not in taken: return c
    return "%04d" % secrets.randbelow(10000)


@app.get("/api/attendance")
@need("admin", "faculty")
def att_get():
    day = valid_day(request.args.get("day"))
    m = {r["pid"]: dict(r) for r in db().execute("SELECT pid,status,via,ts FROM attendance WHERE day=?", (day,)).fetchall()}
    rows = db().execute("SELECT id,name,email,org FROM participants WHERE status='approved' ORDER BY name").fetchall()
    codes = day_codes()
    # every class day gets its own fresh 4-digit code automatically (kept closed until faculty opens check-in)
    if day == now()[:10] and day in {x["date"] for x in course()["sessions"]} and day not in codes:
        set_setting("checkin_" + day, fresh_code({c for c, _ in codes.values()}))
        set_setting("checkin_state_" + day, "closed"); codes = day_codes()
    cur = codes.get(day)
    return jsonify(day=day, day_label=day_label(day), code=cur[0] if cur else None, open=bool(cur and cur[1]),
                   codes=[{"day": d, "label": day_label(d), "code": c, "open": o} for d, (c, o) in sorted(codes.items())],
                   rows=[{**dict(r), "status": m.get(r["id"], {}).get("status", ""),
                          "via": m.get(r["id"], {}).get("via", ""),
                          "ts": m.get(r["id"], {}).get("ts", "")} for r in rows])

def put_att(day, pid, status, via, by):
    old = db().execute("SELECT status FROM attendance WHERE day=? AND pid=?", (day, pid)).fetchone()
    if old and old["status"] == status: return   # unchanged: keep the original check-in time and method
    db().execute("DELETE FROM attendance WHERE day=? AND pid=?", (day, pid))
    if status in ("present", "absent"):
        db().execute("INSERT INTO attendance(day,pid,status,via,marked_by,ts) VALUES(?,?,?,?,?,?)", (day, pid, status, via, by, now()))

@app.post("/api/attendance")
@need("admin", "faculty")
def att_save():
    d = request.get_json(silent=True) or {}; day = valid_day(d.get("day"))
    for pid, st in (d.get("marks") or {}).items():
        if str(pid).isdigit(): put_att(day, int(pid), st, "faculty", session["u"])
    db().commit(); log("attendance_saved", day); return jsonify(ok=True)

@app.post("/api/attendance/checkin_code")
@need("admin", "faculty")
def att_code():
    """Open / update / close self check-in for one day. Every day has its own code; a code can't be reused on another day."""
    d = request.get_json(silent=True) or {}; day = valid_day(d.get("day"))
    codes = day_codes()
    if not d.get("open"):
        if day in codes: set_setting("checkin_state_" + day, "closed"); log("checkin_closed", day)
        return jsonify(ok=True, code=codes.get(day, (None,))[0], open=False)
    code = str(d.get("code") or "").strip()
    taken = {c for k, (c, _) in codes.items() if k != day}
    if not code: code = fresh_code(taken)
    if not re.match(r"^\d{4}$", code): return jsonify(error="The code must be exactly 4 digits"), 400
    if code in taken:
        other = [k for k, (c, _) in codes.items() if k != day and c == code][0]
        return jsonify(error="Code %s is already used for %s. Choose a different code for each day." % (code, day_label(other))), 400
    set_setting("checkin_" + day, code); set_setting("checkin_state_" + day, "open")
    log("checkin_open", "%s code set" % day); return jsonify(ok=True, code=code, open=True)

@app.post("/api/attendance/random_code")
@need("admin", "faculty")
def att_random():
    d = request.get_json(silent=True) or {}; day = valid_day(d.get("day"))
    return jsonify(code=fresh_code({c for k, (c, _) in day_codes().items() if k != day}))

def name_matches(typed, registered):
    words = lambda x: {w for w in re.sub(r"[^a-z0-9 ]", " ", (x or "").lower()).split() if len(w) > 1}
    return bool(words(typed) & words(registered))

@app.post("/api/checkin")  # open self check-in: anyone with today's code can mark attendance; unknown emails are added as participants
def api_checkin():
    if request.headers.get("X-Requested-With") != "fetch": return jsonify(error="Bad request"), 400
    d = request.get_json(silent=True) or {}; day = now()[:10]
    email = clean(d, "email", 160).lower(); typed = clean(d, "name", 120)
    ip_key = "ci|" + client_ip()                    # one device guessing codes with many different emails
    key = client_ip() + "|" + email                 # per-email limit, so one wrong student on shared campus Wi-Fi cannot block the room
    if throttled(key) or throttled(ip_key, 40): return jsonify(error="Too many attempts. Try again in 10 minutes."), 429
    if not typed: return jsonify(error="Please enter your name"), 400
    if not EMAIL_RE.match(email): return jsonify(error="Please enter a valid email address"), 400
    cur = day_codes().get(day)
    if not cur or not cur[1]:
        return jsonify(error="Check-in is closed right now. Please ask your faculty to open it."), 400
    if clean(d, "code", 8) != cur[0]:
        FAILS.setdefault(key, []).append(time.time()); FAILS.setdefault(ip_key, []).append(time.time())
        return jsonify(error="Wrong code. Please enter today's 4-digit code announced by your faculty."), 400
    # anyone may check in: use the existing participant for this email, or add one automatically
    p = db().execute("SELECT id,name,status FROM participants WHERE email=? ORDER BY (status='approved') DESC, id DESC", (email,)).fetchone()
    if p and p["status"] != "approved":
        db().execute("UPDATE participants SET status='approved' WHERE id=?", (p["id"],))
    if not p:
        db().execute("INSERT INTO participants(name,email,phone,org,ref,status,created,note) VALUES(?,?,?,?,?,?,?,?)",
                     (typed, email, "", "", "self-checkin", "approved", now(), "Added automatically via self check-in"))
        p = db().execute("SELECT id,name FROM participants WHERE email=? ORDER BY id DESC", (email,)).fetchone()
    put_att(day, p["id"], "present", "self", email); db().commit(); log("checkin", "%s | %s" % (email, typed), user="-")
    return jsonify(ok=True, name=p["name"])

@app.get("/api/attendance/report")
@need("admin", "faculty")
def att_report():
    days = [r["day"] for r in db().execute("SELECT DISTINCT day FROM attendance ORDER BY day").fetchall()]
    pres = {}
    for r in db().execute("SELECT pid,day,ts FROM attendance WHERE status='present'").fetchall(): pres.setdefault(r["pid"], {})[r["day"]] = str(r["ts"] or "")[11:16]
    rows = db().execute("SELECT id,name,email FROM participants WHERE status='approved' ORDER BY name").fetchall()
    return jsonify(days=days, rows=[{"name": r["name"], "email": r["email"], "present": len(pres.get(r["id"], ())),
                                     "marks": {d: pres.get(r["id"], {}).get(d, "") for d in days}} for r in rows])

# ---------- participants: shared-ID approval + bulk import ----------
@app.post("/api/admin/participants/<int:pid>/approve_shared")
@need("admin")
def a_approve_shared(pid):
    _part(pid); db().execute("UPDATE participants SET status='approved' WHERE id=?", (pid,)); db().commit()
    log("participant_approved_shared", str(pid)); return jsonify(ok=True)

@app.post("/api/admin/participants/bulk")
@need("admin", "faculty")
def a_bulk():
    n = 0
    for line in str((request.get_json(silent=True) or {}).get("text", "")).splitlines():
        p = [x.strip() for x in line.split(",")]
        if len(p) < 2 or not EMAIL_RE.match(p[1].lower()): continue
        if db().execute("SELECT 1 AS x FROM participants WHERE email=?", (p[1].lower(),)).fetchone(): continue
        db().execute("INSERT INTO participants(name,email,phone,org,ref,status,created) VALUES(?,?,?,?,?,?,?)",
                     (p[0][:120], p[1].lower()[:160], (p[2] if len(p) > 2 else "")[:30], (p[3] if len(p) > 3 else "")[:160], "bulk", "approved", now())); n += 1
    db().commit(); log("bulk_import", str(n)); return jsonify(ok=True, added=n)


# ---------- admin ----------
@app.get("/api/admin/users")
@need("admin")
def a_users():
    return jsonify([dict(r) for r in db().execute("SELECT id,username,role,name,active FROM users ORDER BY id").fetchall()])

@app.post("/api/admin/users")
@need("admin")
def a_user_add():
    d = request.get_json(silent=True) or {}
    u = str(d.get("username", "")).strip().lower(); role = d.get("role"); name = clean(d, "name", 120) or u
    if not re.match(r"^[a-z0-9._-]{3,30}$", u): return jsonify(error="Username: 3-30 letters/numbers (a-z, 0-9, . _ -)"), 400
    if role not in ("admin", "faculty", "participant"): return jsonify(error="Choose a role"), 400
    pw = str(d.get("password", "")).strip() or "".join(secrets.choice("abcdefghjkmnpqrstuvwxyz23456789") for _ in range(10))
    if len(pw) < 8: return jsonify(error="Password must be at least 8 characters"), 400
    if db().execute("SELECT 1 AS x FROM users WHERE username=?", (u,)).fetchone(): return jsonify(error="This username already exists"), 400
    db().execute("INSERT INTO users(username,role,name,pw_hash) VALUES(?,?,?,?)", (u, role, name, generate_password_hash(pw))); db().commit()
    log("user_created", "%s (%s)" % (u, role)); return jsonify(ok=True, username=u, password=pw)

@app.delete("/api/admin/users/<int:uid>")
@need("admin")
def a_user_del(uid):
    r = db().execute("SELECT username FROM users WHERE id=?", (uid,)).fetchone()
    if not r or r["username"] == session["u"]: return jsonify(error="You cannot delete yourself"), 400
    db().execute("DELETE FROM users WHERE id=?", (uid,)); db().commit(); log("user_deleted", r["username"]); return jsonify(ok=True)

@app.post("/api/admin/users/<int:uid>/password")
@need("admin")
def a_pw(uid):
    pw = (request.get_json(silent=True) or {}).get("password", "")
    if len(pw) < 8: return jsonify(error="Password must be at least 8 characters"), 400
    db().execute("UPDATE users SET pw_hash=? WHERE id=?", (generate_password_hash(pw), uid)); db().commit()
    log("password_changed", "user id %d" % uid); return jsonify(ok=True)

@app.post("/api/admin/users/<int:uid>/active")
@need("admin")
def a_active(uid):
    on = 1 if (request.get_json(silent=True) or {}).get("active") else 0
    r = db().execute("SELECT username FROM users WHERE id=?", (uid,)).fetchone()
    if not r or (r["username"] == session["u"] and not on): return jsonify(error="You cannot disable yourself"), 400
    db().execute("UPDATE users SET active=? WHERE id=?", (on, uid)); db().commit()
    log("user_enabled" if on else "user_disabled", r["username"]); return jsonify(ok=True)

@app.get("/api/admin/logs")
@need("admin", "faculty")
def a_logs():
    admin = me()["role"] == "admin"; since = now()[:10] + " 00:00:00"
    SAFE = ("login", "download", "upload", "checkin", "announcement_posted", "assignment_published", "assignment_edited", "assignment_deleted", "feedback_form_saved", "day_note_saved")
    if admin: rows = db().execute("SELECT ts,username,action,detail,ip FROM logs ORDER BY id DESC LIMIT 200").fetchall()
    else:  # faculty see learning activity only, not account / password / security events
        rows = db().execute("SELECT ts,username,action,detail,ip FROM logs WHERE action IN (%s) ORDER BY id DESC LIMIT 200" % ",".join("?" * len(SAFE)), SAFE).fetchall()
    cnt = lambda act: db().execute("SELECT COUNT(*) AS n FROM logs WHERE action=? AND ts>=?", (act, since)).fetchone()["n"]
    top = [dict(r) for r in db().execute("SELECT detail, COUNT(*) AS c FROM logs WHERE action='download' AND ts>=? GROUP BY detail ORDER BY c DESC LIMIT 8", (since,)).fetchall()]
    n = lambda s: db().execute(s).fetchone()["n"]
    return jsonify(logs=[dict(r) for r in rows], today=dict(logins=cnt("login"), checkins=cnt("checkin"), downloads=cnt("download"), top=top), stats=dict(
        files=n("SELECT COUNT(*) AS n FROM files"), downloads=n("SELECT COUNT(*) AS n FROM logs WHERE action='download'"),
        logins=n("SELECT COUNT(*) AS n FROM logs WHERE action='login'")))

def https_or_none(u):
    u = (u or "").strip()
    return u if re.match(r"^https://[^\s]+$", u) else None

@app.post("/api/admin/session/<int:n>")
@need("admin")
def a_session(n):
    d = request.get_json(silent=True) or {}
    if not 1 <= n <= 14: abort(404)
    o = jsetting("overrides", {}); sess = o.setdefault("sessions", {})
    sess[str(n)] = {"title": str(d.get("title", "")).strip()[:300], "overview": str(d.get("overview", "")).strip()[:6000],
                    "note": str(d.get("note", "")).strip()[:1000]}
    set_jsetting("overrides", o); log("session_edited", "session %d" % n); return jsonify(ok=True)

@app.delete("/api/admin/session/<int:n>")
@need("admin")
def a_session_reset(n):
    o = jsetting("overrides", {}); o.get("sessions", {}).pop(str(n), None)
    set_jsetting("overrides", o); log("session_restored", "session %d" % n); return jsonify(ok=True)

@app.post("/api/session/<int:n>/links")
@need("admin", "faculty")
def a_link_add(n):
    d = request.get_json(silent=True) or {}; url = https_or_none(d.get("url"))
    if not url or not 1 <= n <= 14: return jsonify(error="Please paste a valid https:// link"), 400
    L = jsetting("links", {}); L.setdefault(str(n), []).append({"label": (d.get("label") or "Recording").strip()[:120], "url": url})
    set_jsetting("links", L); log("link_added", "session %d" % n); return jsonify(ok=True)

@app.delete("/api/session/<int:n>/links/<int:i>")
@need("admin", "faculty")
def a_link_del(n, i):
    L = jsetting("links", {}); arr = L.get(str(n), [])
    if 0 <= i < len(arr): arr.pop(i)
    L[str(n)] = arr; set_jsetting("links", L); log("link_removed", "session %d" % n); return jsonify(ok=True)

@app.post("/api/admin/formurl")
@need("admin", "faculty")
def a_formurl():
    raw = ((request.get_json(silent=True) or {}).get("url") or "").strip()
    if raw and not https_or_none(raw): return jsonify(error="Please paste a valid https:// link"), 400
    set_setting("form_url", raw or None); log("form_link_changed"); return jsonify(ok=True)

# dynamic branding: logos + size, managed from the admin dashboard
@app.get("/api/admin/branding")
@need("admin")
def a_brand(): return jsonify(size=int(setting("logo_size", "70")))

@app.post("/api/admin/branding")
@need("admin")
def a_brand_size():
    s = (request.get_json(silent=True) or {}).get("size")
    if not isinstance(s, int) or not 36 <= s <= 110: return jsonify(error="Size must be 36-110 px"), 400
    set_setting("logo_size", str(s)); return jsonify(ok=True)

@app.post("/api/admin/logo/<int:n>")
@need("admin")
def a_logo(n):
    f = request.files.get("file")
    if n not in (0, 1) or not f: return jsonify(error="Choose an image"), 400
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in IMG: return jsonify(error="Use PNG, JPG or WEBP"), 400
    name = "logos/logo%d-%d.%s" % (n, int(time.time()), ext); old = setting("logo%d" % n)
    store_put(name, f.read(), IMG[ext]); set_setting("logo%d" % n, name); set_setting("logo%d_v" % n, str(int(time.time())))
    if old: store_del(old)
    log("logo_changed", "logo %d" % (n + 1)); return jsonify(ok=True)

@app.delete("/api/admin/logo/<int:n>")
@need("admin")
def a_logo_reset(n):
    old = setting("logo%d" % n)
    if old: store_del(old)
    set_setting("logo%d" % n, None); set_setting("logo%d_v" % n, str(int(time.time()))); return jsonify(ok=True)


app.secret_key = os.getenv("SECRET_KEY") or "dev-only-change-me"
init()

# ---- Keep-alive: on Render free plan the service sleeps after ~15 min without traffic.
# While awake, ping our own public URL every few minutes so it keeps counting as traffic.
# Set KEEPALIVE=0 to switch it off. KEEPALIVE_URL overrides the URL (RENDER_EXTERNAL_URL is automatic on Render).
def _start_keepalive():
    import threading
    if os.getenv("KEEPALIVE", "1") == "0": return
    base = (os.getenv("KEEPALIVE_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").rstrip("/")
    if not base: return
    every = max(60, int(os.getenv("KEEPALIVE_SECONDS", "540")))
    def loop():
        import requests
        while True:
            time.sleep(every)
            try: requests.get(base + "/healthz", timeout=20)
            except Exception: pass
    threading.Thread(target=loop, daemon=True, name="keepalive").start()

_start_keepalive()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
