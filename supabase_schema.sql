-- OPTIONAL: the app creates these tables itself on first start. Run this only if you want to see them first.
CREATE TABLE IF NOT EXISTS users(id SERIAL PRIMARY KEY, username TEXT UNIQUE, role TEXT, name TEXT, pw_hash TEXT, active INTEGER DEFAULT 1);
CREATE TABLE IF NOT EXISTS files(id SERIAL PRIMARY KEY, title TEXT, category TEXT, session_no INTEGER, orig_name TEXT, stored TEXT, size BIGINT, uploaded_by TEXT, created TEXT);
CREATE TABLE IF NOT EXISTS logs(id SERIAL PRIMARY KEY, ts TEXT, username TEXT, action TEXT, detail TEXT, ip TEXT);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS participants(id SERIAL PRIMARY KEY, name TEXT, email TEXT, phone TEXT, org TEXT, ref TEXT, status TEXT, username TEXT, created TEXT, note TEXT);
-- Tables are only accessed by the server (service key), so keep public access closed:
ALTER TABLE users ENABLE ROW LEVEL SECURITY; ALTER TABLE files ENABLE ROW LEVEL SECURITY;
ALTER TABLE logs ENABLE ROW LEVEL SECURITY; ALTER TABLE settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE participants ENABLE ROW LEVEL SECURITY;
