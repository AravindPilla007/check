from flask import Flask, request, render_template_string, send_file
import requests
import os
import sqlite3
from datetime import datetime
import re

app = Flask(__name__)

# xAI API key (replace with your actual API key or use environment variable)
XAI_API_KEY = "your_xai_api_key_here"  # Set this in Colab or use os.environ
XAI_API_URL = "https://api.x.ai/v1/chat/completions"

# SQLite database file
DB_FILE = "metadata.db"

# Current SAS file for download
current_sas_file = None

# Store table name globally
table_name = None

def init_db():
    """Initialize SQLite database and populate with 20 tables' metadata."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tables (
            table_name TEXT PRIMARY KEY
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS columns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT,
            column_name TEXT,
            type TEXT,
            description TEXT,
            FOREIGN KEY (table_name) REFERENCES tables (table_name)
        )
    """)

    # 20 tables' metadata
    tables_data = [
        ("sales_data", [
            ("sale_id", "numeric", "Unique sale identifier"),
            ("product_name", "character", "Name of the product"),
            ("sale_date", "date", "Date of sale"),
            ("amount", "numeric", "Sale amount in USD"),
            ("region", "character", "Sales region")
        ]),
        ("employee_records", [
            ("emp_id", "numeric", "Employee ID"),
            ("first_name", "character", "Employee first name"),
            ("last_name", "character", "Employee last name"),
            ("salary", "numeric", "Annual salary in USD"),
            ("hire_date", "date", "Date of hire"),
            ("department", "character", "Department name")
        ]),
        ("customer_info", [
            ("customer_id", "numeric", "Unique customer ID"),
            ("email", "character", "Customer email address"),
            ("age", "numeric", "Customer age"),
            ("join_date", "date", "Date customer joined")
        ]),
        ("inventory_stock", [
            ("item_id", "numeric", "Unique item ID"),
            ("item_name", "character", "Name of the item"),
            ("quantity", "numeric", "Stock quantity"),
            ("warehouse", "character", "Warehouse location")
        ]),
        ("order_details", [
            ("order_id", "numeric", "Unique order ID"),
            ("customer_id", "numeric", "Customer ID"),
            ("order_date", "date", "Date of order"),
            ("total_amount", "numeric", "Total order amount")
        ]),
        ("product_catalog", [
            ("product_id", "numeric", "Unique product ID"),
            ("category", "character", "Product category"),
            ("price", "numeric", "Product price"),
            ("stock_level", "numeric", "Current stock level")
        ]),
        ("store_locations", [
            ("store_id", "numeric", "Unique store ID"),
            ("city", "character", "City of store"),
            ("state", "character", "State of store"),
            ("open_date", "date", "Store opening date")
        ]),
        ("supplier_info", [
            ("supplier_id", "numeric", "Unique supplier ID"),
            ("supplier_name", "character", "Name of supplier"),
            ("contact_email", "character", "Supplier email"),
            ("rating", "numeric", "Supplier rating (1-5)")
        ]),
        ("transaction_log", [
            ("transaction_id", "numeric", "Unique transaction ID"),
            ("sale_id", "numeric", "Related sale ID"),
            ("transaction_date", "date", "Date of transaction"),
            ("amount", "numeric", "Transaction amount")
        ]),
        ("marketing_campaigns", [
            ("campaign_id", "numeric", "Unique campaign ID"),
            ("campaign_name", "character", "Name of campaign"),
            ("start_date", "date", "Campaign start date"),
            ("budget", "numeric", "Campaign budget")
        ]),
        ("website_traffic", [
            ("visit_id", "numeric", "Unique visit ID"),
            ("user_id", "numeric", "User ID"),
            ("visit_date", "date", "Date of visit"),
            ("page_views", "numeric", "Number of page views")
        ]),
        ("payment_records", [
            ("payment_id", "numeric", "Unique payment ID"),
            ("order_id", "numeric", "Related order ID"),
            ("payment_date", "date", "Date of payment"),
            ("amount", "numeric", "Payment amount")
        ]),
        ("hr_attendance", [
            ("attendance_id", "numeric", "Unique attendance ID"),
            ("emp_id", "numeric", "Employee ID"),
            ("date", "date", "Attendance date"),
            ("hours_worked", "numeric", "Hours worked")
        ]),
        ("logistics_routes", [
            ("route_id", "numeric", "Unique route ID"),
            ("start_location", "character", "Starting location"),
            ("end_location", "character", "Ending location"),
            ("distance", "numeric", "Distance in miles")
        ]),
        ("event_log", [
            ("event_id", "numeric", "Unique event ID"),
            ("event_type", "character", "Type of event"),
            ("event_date", "date", "Date of event"),
            ("user_id", "numeric", "User ID")
        ]),
        ("feedback_survey", [
            ("survey_id", "numeric", "Unique survey ID"),
            ("customer_id", "numeric", "Customer ID"),
            ("score", "numeric", "Feedback score (1-10)"),
            ("comments", "character", "Customer comments")
        ]),
        ("asset_inventory", [
            ("asset_id", "numeric", "Unique asset ID"),
            ("asset_name", "character", "Name of asset"),
            ("purchase_date", "date", "Date of purchase"),
            ("value", "numeric", "Asset value")
        ]),
        ("training_records", [
            ("training_id", "numeric", "Unique training ID"),
            ("emp_id", "numeric", "Employee ID"),
            ("training_date", "date", "Date of training"),
            ("course_name", "character", "Name of course")
        ]),
        ("budget_allocation", [
            ("budget_id", "numeric", "Unique budget ID"),
            ("department", "character", "Department name"),
            ("amount", "numeric", "Budget amount"),
            ("fiscal_year", "numeric", "Fiscal year")
        ]),
        ("support_tickets", [
            ("ticket_id", "numeric", "Unique ticket ID"),
            ("customer_id", "numeric", "Customer ID"),
            ("issue_date", "date", "Date ticket was raised"),
            ("status", "character", "Ticket status")
        ])
    ]

    # Clear existing data
    cursor.execute("DELETE FROM columns")
    cursor.execute("DELETE FROM tables")

    # Insert tables and columns
    for table_name, columns in tables_data:
        cursor.execute("INSERT OR IGNORE INTO tables (table_name) VALUES (?)", (table_name,))
        for col_name, col_type, col_desc in columns:
            cursor.execute(
                "INSERT INTO columns (table_name, column_name, type, description) VALUES (?, ?, ?, ?)",
                (table_name, col_name, col_type, col_desc)
            )

    conn.commit()
    conn.close()

# Initialize database
init_db()

# HTML template for the interface
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>SAS Query Generator</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background-color: #f4f4f9; }
        h1 { color: #333; }
        .container { max-width: 800px; margin: auto; padding: 20px; background: white; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
        .form-group { margin-bottom: 20px; }
        label { font-weight: bold; display: block; margin-bottom: 5px; }
        select, textarea { width: 100%; padding: 10px; margin-top: 5px; border: 1px solid #ccc; border-radius: 4px; }
        textarea { height: 100px; resize: vertical; }
        button { background-color: #28a745; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
        button:hover { background-color: #218838; }
        pre { background: #f8f8f8; padding: 15px; border-radius: 4px; overflow-x: auto; }
        .error { color: red; }
        .success { color: green; }
        .metadata { font-size: 0.9em; color: #555; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Welcome to SAS Query Generator</h1>
        {% if not table_name %}
        <div class="form-group">
            <form method="POST" action="/set_table">
                <label for="table_name">Select Table:</label>
                <select id="table_name" name="table_name" required>
                    {% for table in tables %}
                    <option value="{{ table }}">{{ table }}</option>
                    {% endfor %}
                </select>
                <button type="submit">Set Table</button>
            </form>
        </div>
        {% else %}
        <p><strong>Selected Table:</strong> {{ table_name }}</p>
        <div class="metadata">
            <h3>Table Metadata</h3>
            <table>
                <tr><th>Column</th><th>Type</th><th>Description</th></tr>
                {% for col in metadata %}
                <tr><td>{{ col.column_name }}</td><td>{{ col.type }}</td><td>{{ col.description }}</td></tr>
                {% endfor %}
            </table>
        </div>
        <div class="form-group">
            <form method="POST" action="/generate_query">
                <label for="query">Enter your query in simple English:</label>
                <textarea id="query" name="query" required></textarea>
                <button type="submit">Generate SAS Query</button>
            </form>
        </div>
        {% if sas_code %}
        <h2>Generated SAS PROC SQL Code:</h2>
        <pre>{{ sas_code }}</pre>
        <a href="/download"><button>Download SAS File</button></a>
        {% endif %}
        {% if error %}
        <p class="error">{{ error }}</p>
        {% endif %}
        {% endif %}
        {% if success %}
        <p class="success">{{ success }}</p>
        {% endif %}
    </div>
</body>
</html>
"""

def get_tables():
    """Get list of table names from SQLite."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM tables ORDER BY table_name")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables

def get_table_metadata(table_name):
    """Get metadata for a specific table from SQLite."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        "SELECT column_name, type, description FROM columns WHERE table_name = ?",
        (table_name,)
    )
    metadata = cursor.fetchall()
    conn.close()
    return metadata

def call_grok_api(query, table_name):
    """Call xAI Grok 3 API to convert natural language query to SAS PROC SQL."""
    metadata = get_table_metadata(table_name)
    columns_info = "\n".join([f"- {col['column_name']}: {col['type']} ({col['description']})" for col in metadata])
    prompt = f"""
    You are an expert in SAS PROC SQL. The user is querying a table named '{table_name}' with the following columns:
    {columns_info}

    Convert the following natural language query into a valid SAS PROC SQL query.
    - Ensure the query is syntactically correct and uses the table name and column names provided.
    - Include comments in the SAS code to explain the query's purpose and key steps.
    - Follow SAS PROC SQL conventions (e.g., end with QUIT;).
    - If column names are not explicitly mentioned, infer them based on the query and metadata.
    - If the query is ambiguous, make reasonable assumptions and document them in comments.
    - If the query cannot be converted to a valid SAS PROC SQL query, return exactly: "Query cannot be converted to SAS PROC SQL".
    - Return only the SAS PROC SQL code or the error message, without additional text.

    Query: {query}
    """
    headers = {
        "Authorization": f"Bearer {XAI_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "grok-3",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 500,
        "temperature": 0.3
    }
    try:
        response = requests.post(XAI_API_URL, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        output = result["choices"][0]["message"]["content"].strip()
        return output
    except Exception as e:
        print(f"Error calling xAI API: {e}")
        return "Query cannot be converted to SAS PROC SQL"

def save_sas_file(sas_code):
    """Save SAS code to a file and return the filename."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sas_query_{timestamp}.sas"
    with open(filename, "w") as f:
        f.write(sas_code)
    return filename

@app.route("/", methods=["GET"])
def index():
    global table_name
    tables = get_tables()
    return render_template_string(HTML_TEMPLATE, table_name=table_name, tables=tables)

@app.route("/set_table", methods=["POST"])
def set_table():
    global table_name
    table_name_input = request.form.get("table_name", "").strip()
    tables = get_tables()
    if table_name_input not in tables:
        return render_template_string(
            HTML_TEMPLATE,
            table_name=table_name,
            tables=tables,
            error="Invalid table name. Please select a valid table."
        )
    table_name = table_name_input
    metadata = get_table_metadata(table_name)
    return render_template_string(
        HTML_TEMPLATE,
        table_name=table_name,
        tables=tables,
        metadata=metadata,
        success=f"Table '{table_name}' selected."
    )

@app.route("/generate_query", methods=["POST"])
def generate_query():
    global table_name
    tables = get_tables()
    if not table_name:
        return render_template_string(
            HTML_TEMPLATE,
            table_name=table_name,
            tables=tables,
            error="Please select a table first."
        )
    
    query = request.form.get("query", "").strip()
    if not query:
        return render_template_string(
            HTML_TEMPLATE,
            table_name=table_name,
            tables=tables,
            metadata=get_table_metadata(table_name),
            error="Query cannot be empty."
        )
    
    sas_code = call_grok_api(query, table_name)
    if sas_code == "Query cannot be converted to SAS PROC SQL":
        return render_template_string(
            HTML_TEMPLATE,
            table_name=table_name,
            tables=tables,
            metadata=get_table_metadata(table_name),
            error="Query cannot be converted to SAS PROC SQL."
        )
    
    # Save the SAS code to a file
    filename = save_sas_file(sas_code)
    global current_sas_file
    current_sas_file = filename
    
    return render_template_string(
        HTML_TEMPLATE,
        table_name=table_name,
        tables=tables,
        metadata=get_table_metadata(table_name),
        sas_code=sas_code
    )

@app.route("/download", methods=["GET"])
def download():
    global current_sas_file
    tables = get_tables()
    if not current_sas_file or not os.path.exists(current_sas_file):
        return render_template_string(
            HTML_TEMPLATE,
            table_name=table_name,
            tables=tables,
            metadata=get_table_metadata(table_name) if table_name else [],
            error="No SAS file available for download."
        )
    return send_file(current_sas_file, as_attachment=True)

if __name__ == "__main__":
    from pyngrok import ngrok
    ngrok.set_auth_token("your_ngrok_authtoken_here")
    public_url = ngrok.connect(5000).public_url
    print(f"Flask app running at: {public_url}")
    app.run(port=5000)
