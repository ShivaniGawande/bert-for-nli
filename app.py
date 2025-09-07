# app.py
from flask import Flask, request, redirect, url_for, render_template_string, flash
from werkzeug.utils import secure_filename
import pandas as pd
from io import BytesIO

from dq_health_check import DataSet, rules_from_df, run_health_check

app = Flask(__name__)
app.secret_key = "dev"  # replace in production
ALLOWED = {".xlsx", ".xls"}
MAX_MB = 25
app.config["MAX_CONTENT_LENGTH"] = MAX_MB * 1024 * 1024

HTML = """
<!doctype html>
<title>DQ Health Check</title>
<h2>Upload your sources (Excel files)</h2>
<form method="post" action="{{ url_for('analyze') }}" enctype="multipart/form-data">
  <p>
    <input type="file" name="sources" multiple accept=".xlsx,.xls" />
  </p>
  <p>
    <label>Main source index (1..N, optional):</label>
    <input type="number" name="main_index" min="1" step="1">
  </p>
  <button type="submit">Analyze</button>
</form>

{% if report %}
  <hr>
  <h3>Results</h3>
  <p><b>Main source:</b> {{ report.main_source }}</p>
  <p><b>Rule Count:</b> {{ report.rule_count_msg }}</p>

  {% if report.missing_headers %}
    <h4>{{ report.emoji.FAIL }} Missing Headers</h4>
    <ul>
    {% for src, fields in report.missing_headers.items() %}
      <li><b>{{ src }}</b>: {{ ", ".join(fields) }}</li>
    {% endfor %}
    </ul>
  {% else %}
    <p>{{ report.emoji.OK }} No missing headers.</p>
  {% endif %}

  {% if report.exclusives %}
    <h4>{{ report.emoji.INFO }} Exclusive Rules (only in that source)</h4>
    <ul>
    {% for src, rules in report.exclusives.items() %}
      <li><b>{{ src }}</b>: {{ ", ".join(rules) }}</li>
    {% endfor %}
    </ul>
  {% else %}
    <p>{{ report.emoji.OK }} No exclusive rules.</p>
  {% endif %}

  {% if report.mismatches %}
    <h4>{{ report.emoji.FAIL }} Sync Mismatches (headers differ)</h4>
    <ul>
    {% for src, rules in report.mismatches.items() %}
      <li><b>{{ src }}</b>: {{ ", ".join(rules) }}</li>
    {% endfor %}
    </ul>
  {% else %}
    <p>{{ report.emoji.OK }} No sync mismatches.</p>
  {% endif %}

  <hr>
  {% if report.ok %}
    <h3>{{ report.emoji.OK }} All checks passed.</h3>
  {% else %}
    <h3>{{ report.emoji.INFO }} Please review the items above.</h3>
  {% endif %}
{% endif %}
"""

def _is_allowed(filename: str) -> bool:
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in ALLOWED)

@app.get("/")
def index():
    return render_template_string(HTML, report=None)

@app.post("/analyze")
def analyze():
    files = request.files.getlist("sources")
    if not files or all(f.filename.strip() == "" for f in files):
        flash("Please upload at least one Excel file.")
        return redirect(url_for("index"))

    # Build DataSet objects (in-memory)
    sources = {}
    for i, file in enumerate(files, start=1):
        filename = secure_filename(file.filename or f"source_{i}.xlsx")
        if not _is_allowed(filename):
            flash(f"Unsupported file type: {filename}")
            return redirect(url_for("index"))

        # Read the Excel into a DataFrame (first sheet by default)
        data = file.read()
        df = pd.read_excel(BytesIO(data))   # requires openpyxl for .xlsx

        # Interpret the uploaded sheet itself as the "rules" list
        rules = rules_from_df(df)

        # You can derive fields from headers if you keep that concept:
        fields = {h: {"header": h} for h in df.columns}

        sources[f"s{i}"] = DataSet(
            name=filename,
            dataframe=df,
            dq_rules=rules,
            fields=fields
        )

    # Choose main source if provided
    main_index = request.form.get("main_index", "").strip()
    main_key = None
    if main_index.isdigit():
        idx = int(main_index) - 1
        keys = list(sources.keys())
        if 0 <= idx < len(keys):
            main_key = keys[idx]

    report = run_health_check(sources, main_key=main_key)
    # Make dict accessible in template with attribute-style access
    class Obj(dict):
        __getattr__ = dict.get
    return render_template_string(HTML, report=Obj(report))

if __name__ == "__main__":
    # Run locally
    app.run(debug=True)
