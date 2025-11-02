import os
import sqlite3
from flask import Flask, render_template_string, g

app = Flask(__name__)
DATABASE = "data/scraper.db"

MINIMAL_CSS = """
<style>
body { font-family: Arial, sans-serif; background: #181818; color: #eee; margin:0; padding:0;}
nav { background:#222; padding:1em; margin-bottom:2em;}
nav a { color:#eee; margin-right:1em; text-decoration:none; }
table { width:100%; border-collapse:collapse; margin-bottom:2em; }
th, td { border:1px solid #444; padding:8px; text-align:left;}
th { background:#333; }
tr:nth-child(even) { background:#222;}
.container{ max-width:1100px; margin:auto; padding:1em;}
h1, h2{ color:#00bfff; }
img { max-height:35px; }
</style>
"""

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

HOME_TEMPLATE = MINIMAL_CSS + """
<nav>
    <a href="/">Home</a>
    <a href="/shows">Shows</a>
    <a href="/seasons">Seasons</a>
    <a href="/episodes">Episodes</a>
    <a href="/servers">Servers</a>
</nav>
<div class="container">
    <h1>Database Viewer</h1>
    <ul>
        <li><a href="/shows">View Shows</a></li>
        <li><a href="/seasons">View Seasons</a></li>
        <li><a href="/episodes">View Episodes</a></li>
        <li><a href="/servers">View Servers</a></li>
    </ul>
</div>
"""

SHOWS_TEMPLATE = MINIMAL_CSS + """
<nav>
    <a href="/">Home</a>
    <a href="/shows">Shows</a>
    <a href="/seasons">Seasons</a>
    <a href="/episodes">Episodes</a>
    <a href="/servers">Servers</a>
</nav>
<div class="container">
    <h2>Shows</h2>
    <table>
    <tr>
        <th>ID</th>
        <th>Title</th>
        <th>Type</th>
        <th>Year</th>
        <th>IMDb Rating</th>
        <th>Poster</th>
    </tr>
    {% for show in shows %}
    <tr>
        <td>{{ show['id'] }}</td>
        <td>{{ show['title'] }}</td>
        <td>{{ show['type'] }}</td>
        <td>{{ show['year'] or '' }}</td>
        <td>{{ show['imdb_rating'] or '' }}</td>
        <td>{% if show['poster'] %}<img src="{{ show['poster'] }}" alt="">{% endif %}</td>
    </tr>
    {% endfor %}
    </table>
</div>
"""

SEASONS_TEMPLATE = MINIMAL_CSS + """
<nav>
    <a href="/">Home</a>
    <a href="/shows">Shows</a>
    <a href="/seasons">Seasons</a>
    <a href="/episodes">Episodes</a>
    <a href="/servers">Servers</a>
</nav>
<div class="container">
    <h2>Seasons</h2>
    <table>
        <tr><th>ID</th><th>Show ID</th><th>Season Number</th><th>Poster</th></tr>
        {% for s in seasons %}
        <tr>
            <td>{{ s['id'] }}</td>
            <td>{{ s['show_id'] }}</td>
            <td>{{ s['season_number'] }}</td>
            <td>{% if s['poster'] %}<img src="{{ s['poster'] }}" alt="">{% endif %}</td>
        </tr>
        {% endfor %}
    </table>
</div>
"""

EPISODES_TEMPLATE = MINIMAL_CSS + """
<nav>
    <a href="/">Home</a>
    <a href="/shows">Shows</a>
    <a href="/seasons">Seasons</a>
    <a href="/episodes">Episodes</a>
    <a href="/servers">Servers</a>
</nav>
<div class="container">
    <h2>Episodes</h2>
    <table>
        <tr><th>ID</th><th>Season ID</th><th>Episode Number</th></tr>
        {% for e in episodes %}
        <tr>
            <td>{{ e['id'] }}</td>
            <td>{{ e['season_id'] }}</td>
            <td>{{ e['episode_number'] }}</td>
        </tr>
        {% endfor %}
    </table>
</div>
"""

SERVERS_TEMPLATE = MINIMAL_CSS + """
<nav>
    <a href="/">Home</a>
    <a href="/shows">Shows</a>
    <a href="/seasons">Seasons</a>
    <a href="/episodes">Episodes</a>
    <a href="/servers">Servers</a>
</nav>
<div class="container">
    <h2>Servers</h2>
    <table>
        <tr><th>ID</th><th>Episode ID</th><th>Server Num</th><th>Embed URL</th></tr>
        {% for s in servers %}
        <tr>
            <td>{{ s['id'] }}</td>
            <td>{{ s['episode_id'] }}</td>
            <td>{{ s['server_number'] }}</td>
            <td>{% if s['embed_url'] %}<a href="{{ s['embed_url'] }}" target="_blank" style="color:#00bfff;">Embed</a>{% endif %}</td>
        </tr>
        {% endfor %}
    </table>
</div>
"""

@app.route("/")
def home():
    return render_template_string(HOME_TEMPLATE)

@app.route("/shows")
def shows():
    db = get_db()
    shows = db.execute("SELECT id, title, type, year, imdb_rating, poster FROM shows ORDER BY id DESC LIMIT 100").fetchall()
    return render_template_string(SHOWS_TEMPLATE, shows=shows)

@app.route("/seasons")
def seasons():
    db = get_db()
    seasons = db.execute("SELECT id, show_id, season_number, poster FROM seasons ORDER BY id DESC LIMIT 100").fetchall()
    return render_template_string(SEASONS_TEMPLATE, seasons=seasons)

@app.route("/episodes")
def episodes():
    db = get_db()
    episodes = db.execute("SELECT id, season_id, episode_number FROM episodes ORDER BY id DESC LIMIT 100").fetchall()
    return render_template_string(EPISODES_TEMPLATE, episodes=episodes)

@app.route("/servers")
def servers():
    db = get_db()
    servers = db.execute("SELECT id, episode_id, server_number, embed_url FROM servers ORDER BY id DESC LIMIT 100").fetchall()
    return render_template_string(SERVERS_TEMPLATE, servers=servers)

if __name__ == "__main__":
    if not os.path.exists(DATABASE):
        raise RuntimeError(f"Database file not found at {DATABASE}")
    app.run(host="0.0.0.0", port=3000, debug=True)
