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
class Conn:
    def __init__(self):
        if PG:
            import psycopg
            from psycopg.rows import dict_row
            self.c = psycopg.connect(DATABASE_URL, row_factory=dict_row)
        else:
            os.makedirs(DATA, exist_ok=True)
            self.c = sqlite3.connect(os.path.join(DATA, "mmj.db"))
            self.c.row_factory = sqlite3.Row

    def execute(self, sql, args=()):
        return self.c.execute(sql.replace("?", "%s") if PG else sql, args)

    def commit(self): self.c.commit()
    def close(self): self.c.close()


def db():
    if "db" not in g:
        g.db = Conn()
    return g.db


@app.teardown_appcontext
def close_db(_):
    d = g.pop("db", None)
    if d: d.close()


def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


def init():
    c = Conn(); idt = "SERIAL PRIMARY KEY" if PG else "INTEGER PRIMARY KEY"
    for s in ["CREATE TABLE IF NOT EXISTS users(id %s, username TEXT UNIQUE, role TEXT, name TEXT, pw_hash TEXT, active INTEGER DEFAULT 1)" % idt,
              "CREATE TABLE IF NOT EXISTS files(id %s, title TEXT, category TEXT, session_no INTEGER, orig_name TEXT, stored TEXT, size BIGINT, uploaded_by TEXT, created TEXT)" % idt,
              "CREATE TABLE IF NOT EXISTS logs(id %s, ts TEXT, username TEXT, action TEXT, detail TEXT, ip TEXT)" % idt,
              "CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT)",
              "CREATE TABLE IF NOT EXISTS visits(id %s, day TEXT, vid TEXT, path TEXT, ts TEXT)" % idt,
              "CREATE TABLE IF NOT EXISTS attendance(id %s, day TEXT, pid INTEGER, status TEXT, via TEXT, marked_by TEXT, ts TEXT, UNIQUE(day,pid))" % idt,
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
    r = db().execute("SELECT v FROM settings WHERE k=?", (k,)).fetchone()
    return r["v"] if r else default


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
    db().commit()


# ---------- auth ----------
def me():
    if "u" not in session: return None
    r = db().execute("SELECT username,role,name,active FROM users WHERE username=?", (session["u"],)).fetchone()
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
def throttled(ip):
    t = time.time(); FAILS[ip] = [x for x in FAILS.get(ip, []) if t - x < 600]
    return len(FAILS[ip]) >= 8


def client_ip():
    return (request.headers.get("X-Forwarded-For", request.remote_addr) or "").split(",")[0].strip()


# ---------- pages ----------
def track(path):
    try:
        vid = __import__("hashlib").sha256((client_ip() + request.headers.get("User-Agent", "")).encode()).hexdigest()[:16]
        day = now()[:10]
        if not db().execute("SELECT 1 AS x FROM visits WHERE day=? AND vid=? AND path=?", (day, vid, path)).fetchone():
            db().execute("INSERT INTO visits(day,vid,path,ts) VALUES(?,?,?,?)", (day, vid, path, now())); db().commit()
    except Exception: pass


@app.route("/")
def home():
    track("/")
    u = me()
    return render_template("portal.html", role=u["role"]) if u else redirect("/login")

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
        return Response(it, mimetype=ct, headers={"Cache-Control": "public, max-age=300"})
    return send_from_directory(os.path.join(BASE, "static"), "logo%d.jpg" % n)


@app.errorhandler(413)
def too_big(_): return jsonify(error="File is larger than %d MB. For long videos, add a link instead." % MAX_MB), 413


# ---------- auth API ----------
@app.post("/api/login")
def api_login():
    ip = client_ip()
    if throttled(ip): return jsonify(error="Too many attempts. Try again in 10 minutes."), 429
    d = request.get_json(silent=True) or {}
    r = db().execute("SELECT * FROM users WHERE username=?", (str(d.get("username", "")).strip().lower(),)).fetchone()
    if not r or not r["active"] or not check_password_hash(r["pw_hash"], str(d.get("password", ""))):
        FAILS.setdefault(ip, []).append(time.time())
        log("login_failed", str(d.get("username", ""))[:40], user="-")
        return jsonify(error="Wrong ID or password"), 401
    session.clear(); session.permanent = True; session["u"] = r["username"]; log("login")
    return jsonify(ok=True, redirect="/")

@app.post("/api/logout")
def api_logout(): session.clear(); return jsonify(ok=True)

@app.get("/api/me")
def api_me():
    u = me(); return jsonify(user=dict(u) if u else None, categories=CATEGORIES, max_mb=MAX_MB)


# ---------- content ----------
def course(): return json.load(open(os.path.join(DATA, "course.json"), encoding="utf-8"))

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
    return jsonify(json.load(open(os.path.join(DATA, "foundation.json"), encoding="utf-8")))

@app.get("/api/settings")
@need()
def api_settings():
    return jsonify(overrides=jsetting("overrides", {}), links=jsetting("links", {}), formUrl=setting("form_url", ""))

ASSET_FILES = {"brochure": "Mind_Machine_Justice_2026_Course_Brochure.pdf", "primer": "Mind_Machine_Justice_2026_Participant_Primer.pdf"}

@app.get("/api/asset/<key>")
@need()
def api_asset(key):
    if key == "cover": return send_from_directory(os.path.join(BASE, "static"), "brochure-cover.png")
    if key not in ASSET_FILES: abort(404)
    log("download", ASSET_FILES[key])
    return send_from_directory(DATA, ASSET_FILES[key], as_attachment=True)


# ---------- files ----------
@app.get("/api/files")
@need()
def api_files():
    rows = db().execute("SELECT id,title,category,session_no,orig_name,size,uploaded_by,created FROM files ORDER BY id DESC").fetchall()
    return jsonify([dict(r) for r in rows])

@app.post("/api/files")
@need("admin", "faculty")
def api_upload():
    f = request.files.get("file"); title = (request.form.get("title") or "").strip()
    cat = request.form.get("category", "Other")
    if not f or not f.filename: return jsonify(error="Choose a file"), 400
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED: return jsonify(error="File type .%s not allowed" % ext), 400
    if cat not in CATEGORIES: cat = "Other"
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
    log("download", r["title"]); it, ct, _, extra = store_get(r["stored"])
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
    db().execute("DELETE FROM participants WHERE id=?", (pid,)); db().commit()
    log("participant_deleted", p["email"]); return jsonify(ok=True)


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
        attendance_rate=att_rate(),
        series=series)

# ---------- attendance (faculty + admin) ----------
def valid_day(d):
    return d if re.match(r"^\d{4}-\d{2}-\d{2}$", d or "") else now()[:10]

@app.get("/api/attendance")
@need("admin", "faculty")
def att_get():
    day = valid_day(request.args.get("day"))
    m = {r["pid"]: dict(r) for r in db().execute("SELECT pid,status,via FROM attendance WHERE day=?", (day,)).fetchall()}
    rows = db().execute("SELECT id,name,email,org FROM participants WHERE status='approved' ORDER BY name").fetchall()
    return jsonify(day=day, code=setting("checkin_" + day), rows=[{**dict(r), "status": m.get(r["id"], {}).get("status", ""),
                                                                   "via": m.get(r["id"], {}).get("via", "")} for r in rows])

def put_att(day, pid, status, via, by):
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
    d = request.get_json(silent=True) or {}; day = valid_day(d.get("day"))
    code = "%04d" % secrets.randbelow(10000) if d.get("open") else None
    set_setting("checkin_" + day, code); log("checkin_open" if code else "checkin_closed", day); return jsonify(ok=True, code=code)

@app.post("/api/checkin")  # students self check-in (shared ID: they identify with registered email + today's code)
def api_checkin():
    if request.headers.get("X-Requested-With") != "fetch": return jsonify(error="Bad request"), 400
    ip = client_ip()
    if throttled(ip): return jsonify(error="Too many attempts. Try later."), 429
    d = request.get_json(silent=True) or {}; day = now()[:10]
    code = setting("checkin_" + day); email = clean(d, "email", 160).lower()
    p = db().execute("SELECT id,name FROM participants WHERE email=? AND status='approved'", (email,)).fetchone()
    if not code or clean(d, "code", 8) != code or not p:
        FAILS.setdefault(ip, []).append(time.time())
        return jsonify(error="Check-in is closed, code is wrong, or this email is not registered"), 400
    put_att(day, p["id"], "present", "self", email); db().commit(); log("checkin", email, user="-")
    return jsonify(ok=True, name=p["name"])

@app.get("/api/attendance/report")
@need("admin", "faculty")
def att_report():
    days = [r["day"] for r in db().execute("SELECT DISTINCT day FROM attendance ORDER BY day").fetchall()]
    pres = {}
    for r in db().execute("SELECT pid,day FROM attendance WHERE status='present'").fetchall(): pres.setdefault(r["pid"], set()).add(r["day"])
    rows = db().execute("SELECT id,name,email FROM participants WHERE status='approved' ORDER BY name").fetchall()
    return jsonify(days=days, rows=[{"name": r["name"], "email": r["email"], "present": len(pres.get(r["id"], ())),
                                     "marks": {d: (d in pres.get(r["id"], ())) for d in days}} for r in rows])

# ---------- participants: shared-ID approval + bulk import ----------
@app.post("/api/admin/participants/<int:pid>/approve_shared")
@need("admin")
def a_approve_shared(pid):
    _part(pid); db().execute("UPDATE participants SET status='approved' WHERE id=?", (pid,)); db().commit()
    log("participant_approved_shared", str(pid)); return jsonify(ok=True)

@app.post("/api/admin/participants/bulk")
@need("admin")
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
@need("admin")
def a_logs():
    rows = db().execute("SELECT ts,username,action,detail,ip FROM logs ORDER BY id DESC LIMIT 200").fetchall()
    n = lambda s: db().execute(s).fetchone()["n"]
    return jsonify(logs=[dict(r) for r in rows], stats=dict(
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
@need("admin")
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
