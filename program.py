from flask import Flask, request, render_template_string, send_file
import google.generativeai as genai
import os
import sqlite3
from datetime import datetime
import time
from pyngrok import ngrok
import subprocess
from requests.exceptions import HTTPError
import logging

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Gemini API key (replace with your actual API key or use environment variable)
GEMINI_API_KEY = "your_gemini_api_key_here"  # Set this in Colab or use os.environ
MODEL_NAME = "gemini-1.5-flash"

# Configure Gemini API
genai.configure(api_key=GEMINI_API_KEY)

# SQLite database file
DB_FILE = "metadata.db"

# Current SAS file for download
current_sas_file = None

# Store table name globally
table_name = None

# Cache for table explanations and suggestions
explanation_cache = {}
suggestions_cache = {}

def init_db():
    """Initialize SQLite database and populate with 20 tables, each with 10 columns."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

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

    tables_data = [
        ("sales_data", [
            ("sale_id", "numeric", "Unique sale identifier"),
            ("product_name", "character", "Name of the product"),
            ("sale_date", "date", "Date of sale"),
            ("amount", "numeric", "Sale amount in USD"),
            ("region", "character", "Sales region"),
            ("customer_id", "numeric", "Unique customer ID"),
            ("store_id", "numeric", "Unique store ID"),
            ("category", "character", "Product category"),
            ("quantity", "numeric", "Quantity sold"),
            ("discount", "numeric", "Discount applied in USD")
        ]),
        ("employee_records", [
            ("emp_id", "numeric", "Employee ID"),
            ("first_name", "character", "Employee first name"),
            ("last_name", "character", "Employee last name"),
            ("salary", "numeric", "Annual salary in USD"),
            ("hire_date", "date", "Date of hire"),
            ("department", "character", "Department name"),
            ("email", "character", "Employee email"),
            ("phone", "character", "Employee phone number"),
            ("job_title", "character", "Job title"),
            ("manager_id", "numeric", "Manager’s employee ID")
        ]),
        ("customer_info", [
            ("customer_id", "numeric", "Unique customer ID"),
            ("email", "character", "Customer email address"),
            ("age", "numeric", "Customer age"),
            ("join_date", "date", "Date customer joined"),
            ("first_name", "character", "Customer first name"),
            ("last_name", "character", "Customer last name"),
            ("phone", "character", "Customer phone number"),
            ("address", "character", "Customer address"),
            ("loyalty_points", "numeric", "Loyalty program points"),
            ("status", "character", "Customer status (active/inactive)")
        ]),
        ("inventory_stock", [
            ("item_id", "numeric", "Unique item ID"),
            ("item_name", "character", "Name of the item"),
            ("quantity", "numeric", "Stock quantity"),
            ("warehouse", "character", "Warehouse location"),
            ("last_updated", "date", "Date of last stock update"),
            ("unit_price", "numeric", "Price per unit"),
            ("category", "character", "Item category"),
            ("supplier_id", "numeric", "Supplier ID"),
            ("reorder_level", "numeric", "Minimum stock level"),
            ("status", "character", "Stock status (available/low)")
        ]),
        ("order_details", [
            ("order_id", "numeric", "Unique order ID"),
            ("customer_id", "numeric", "Customer ID"),
            ("order_date", "date", "Date of order"),
            ("total_amount", "numeric", "Total order amount"),
            ("status", "character", "Order status"),
            ("shipping_address", "character", "Shipping address"),
            ("payment_method", "character", "Payment method"),
            ("item_count", "numeric", "Number of items"),
            ("discount", "numeric", "Discount applied"),
            ("delivery_date", "date", "Expected delivery date")
        ]),
        ("product_catalog", [
            ("product_id", "numeric", "Unique product ID"),
            ("category", "character", "Product category"),
            ("price", "numeric", "Product price"),
            ("stock_level", "numeric", "Current stock level"),
            ("product_name", "character", "Name of the product"),
            ("description", "character", "Product description"),
            ("brand", "character", "Product brand"),
            ("launch_date", "date", "Product launch date"),
            ("weight", "numeric", "Product weight in kg"),
            ("rating", "numeric", "Average customer rating")
        ]),
        ("store_locations", [
            ("store_id", "numeric", "Unique store ID"),
            ("city", "character", "City of store"),
            ("state", "character", "State of store"),
            ("open_date", "date", "Store opening date"),
            ("address", "character", "Store address"),
            ("manager_id", "numeric", "Store manager ID"),
            ("phone", "character", "Store contact number"),
            ("size", "numeric", "Store size in sq ft"),
            ("region", "character", "Sales region"),
            ("status", "character", "Store status (open/closed)")
        ]),
        ("supplier_info", [
            ("supplier_id", "numeric", "Unique supplier ID"),
            ("supplier_name", "character", "Name of supplier"),
            ("contact_email", "character", "Supplier email"),
            ("rating", "numeric", "Supplier rating (1-5)"),
            ("phone", "character", "Supplier phone number"),
            ("address", "character", "Supplier address"),
            ("contract_date", "date", "Contract start date"),
            ("product_category", "character", "Supplied product category"),
            ("delivery_time", "numeric", "Average delivery time in days"),
            ("status", "character", "Supplier status (active/inactive)")
        ]),
        ("transaction_log", [
            ("transaction_id", "numeric", "Unique transaction ID"),
            ("sale_id", "numeric", "Related sale ID"),
            ("transaction_date", "date", "Date of transaction"),
            ("amount", "numeric", "Transaction amount"),
            ("payment_method", "character", "Payment method"),
            ("customer_id", "numeric", "Customer ID"),
            ("store_id", "numeric", "Store ID"),
            ("status", "character", "Transaction status"),
            ("currency", "character", "Transaction currency"),
            ("notes", "character", "Additional notes")
        ]),
        ("marketing_campaigns", [
            ("campaign_id", "numeric", "Unique campaign ID"),
            ("campaign_name", "character", "Name of campaign"),
            ("start_date", "date", "Campaign start date"),
            ("budget", "numeric", "Campaign budget"),
            ("end_date", "date", "Campaign end date"),
            ("target_audience", "character", "Target audience"),
            ("channel", "character", "Marketing channel"),
            ("roi", "numeric", "Return on investment"),
            ("status", "character", "Campaign status"),
            ("manager_id", "numeric", "Campaign manager ID")
        ]),
        ("website_traffic", [
            ("visit_id", "numeric", "Unique visit ID"),
            ("user_id", "numeric", "User ID"),
            ("visit_date", "date", "Date of visit"),
            ("page_views", "numeric", "Number of page views"),
            ("session_duration", "numeric", "Session duration in seconds"),
            ("source", "character", "Traffic source"),
            ("device", "character", "Device type"),
            ("browser", "character", "Browser used"),
            ("location", "character", "Visitor location"),
            ("conversion", "character", "Conversion status (yes/no)")
        ]),
        ("payment_records", [
            ("payment_id", "numeric", "Unique payment ID"),
            ("order_id", "numeric", "Related order ID"),
            ("payment_date", "date", "Date of payment"),
            ("amount", "numeric", "Payment amount"),
            ("payment_method", "character", "Payment method"),
            ("status", "character", "Payment status"),
            ("customer_id", "numeric", "Customer ID"),
            ("currency", "character", "Payment currency"),
            ("transaction_id", "numeric", "Transaction ID"),
            ("notes", "character", "Additional notes")
        ]),
        ("hr_attendance", [
            ("attendance_id", "numeric", "Unique attendance ID"),
            ("emp_id", "numeric", "Employee ID"),
            ("date", "date", "Attendance date"),
            ("hours_worked", "numeric", "Hours worked"),
            ("status", "character", "Attendance status"),
            ("shift", "character", "Shift type"),
            ("location", "character", "Work location"),
            ("overtime_hours", "numeric", "Overtime hours"),
            ("leave_type", "character", "Leave type if absent"),
            ("notes", "character", "Additional notes")
        ]),
        ("logistics_routes", [
            ("route_id", "numeric", "Unique route ID"),
            ("start_location", "character", "Starting location"),
            ("end_location", "character", "Ending location"),
            ("distance", "numeric", "Distance in miles"),
            ("travel_time", "numeric", "Travel time in hours"),
            ("vehicle_id", "numeric", "Vehicle ID"),
            ("driver_id", "numeric", "Driver ID"),
            ("status", "character", "Route status"),
            ("last_updated", "date", "Last update date"),
            ("cost", "numeric", "Route cost in USD")
        ]),
        ("event_log", [
            ("event_id", "numeric", "Unique event ID"),
            ("event_type", "character", "Type of event"),
            ("event_date", "date", "Date of event"),
            ("user_id", "numeric", "User ID"),
            ("description", "character", "Event description"),
            ("status", "character", "Event status"),
            ("location", "character", "Event location"),
            ("duration", "numeric", "Event duration in minutes"),
            ("priority", "character", "Event priority"),
            ("notes", "character", "Additional notes")
        ]),
        ("feedback_survey", [
            ("survey_id", "numeric", "Unique survey ID"),
            ("customer_id", "numeric", "Customer ID"),
            ("score", "numeric", "Feedback score (1-10)"),
            ("comments", "character", "Customer comments"),
            ("survey_date", "date", "Date of survey"),
            ("product_id", "numeric", "Related product ID"),
            ("category", "character", "Survey category"),
            ("status", "character", "Survey status"),
            ("response_time", "numeric", "Response time in hours"),
            ("follow_up", "character", "Follow-up required (yes/no)")
        ]),
        ("asset_inventory", [
            ("asset_id", "numeric", "Unique asset ID"),
            ("asset_name", "character", "Name of asset"),
            ("purchase_date", "date", "Date of purchase"),
            ("value", "numeric", "Asset value"),
            ("location", "character", "Asset location"),
            ("status", "character", "Asset status"),
            ("category", "character", "Asset category"),
            ("last_maintenance", "date", "Last maintenance date"),
            ("depreciation", "numeric", "Depreciation amount"),
            ("owner_id", "numeric", "Owner employee ID")
        ]),
        ("training_records", [
            ("training_id", "numeric", "Unique training ID"),
            ("emp_id", "numeric", "Employee ID"),
            ("training_date", "date", "Date of training"),
            ("course_name", "character", "Name of course"),
            ("duration", "numeric", "Duration in hours"),
            ("trainer_id", "numeric", "Trainer ID"),
            ("status", "character", "Training status"),
            ("cost", "numeric", "Training cost"),
            ("location", "character", "Training location"),
            ("certificate", "character", "Certificate issued (yes/no)")
        ]),
        ("budget_allocation", [
            ("budget_id", "numeric", "Unique budget ID"),
            ("department", "character", "Department name"),
            ("amount", "numeric", "Budget amount"),
            ("fiscal_year", "numeric", "Fiscal year"),
            ("start_date", "date", "Budget start date"),
            ("end_date", "date", "Budget end date"),
            ("manager_id", "numeric", "Manager ID"),
            ("status", "character", "Budget status"),
            ("category", "character", "Budget category"),
            ("spent_amount", "numeric", "Amount spent")
        ]),
        ("support_tickets", [
            ("ticket_id", "numeric", "Unique ticket ID"),
            ("customer_id", "numeric", "Customer ID"),
            ("issue_date", "date", "Date ticket was raised"),
            ("status", "character", "Ticket status"),
            ("description", "character", "Issue description"),
            ("priority", "character", "Ticket priority"),
            ("assigned_to", "numeric", "Assigned employee ID"),
            ("resolution_date", "date", "Date resolved"),
            ("category", "character", "Issue category"),
            ("satisfaction_score", "numeric", "Customer satisfaction score")
        ])
    ]

    cursor.execute("DELETE FROM columns")
    cursor.execute("DELETE FROM tables")

    for table_name, columns in tables_data:
        cursor.execute("INSERT OR IGNORE INTO tables (table_name) VALUES (?)", (table_name,))
        for col_name, col_type, col_desc in columns:
            cursor.execute(
                "INSERT INTO columns (table_name, column_name, type, description) VALUES (?, ?, ?, ?)",
                (table_name, col_name, col_type, col_desc)
            )

    conn.commit()
    conn.close()

init_db()

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>SAS Query Generator</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .column-row { display: none; }
        .column-row.visible { display: table-row; }
        .suggestion { cursor: pointer; }
        .suggestion:hover { background-color: #e5e7eb; }
    </style>
</head>
<body class="bg-gray-100 font-sans">
    <div class="container max-w-4xl mx-auto p-6 bg-white rounded-lg shadow-lg">
        <h1 class="text-3xl font-bold text-gray-800 mb-6">Welcome to SAS Query Generator</h1>
        {% if not table_name %}
        <div class="mb-6">
            <form method="POST" action="/set_table" class="space-y-4">
                <label for="table_name" class="block text-sm font-medium text-gray-700">Select Table</label>
                <select id="table_name" name="table_name" required class="w-full p-2 border rounded-md">
                    {% for table in tables %}
                    <option value="{{ table }}">{{ table }}</option>
                    {% endfor %}
                </select>
                <button type="submit" class="bg-green-500 text-white px-4 py-2 rounded-md hover:bg-green-600">Set Table</button>
            </form>
        </div>
        {% else %}
        <div class="flex justify-between items-center mb-4">
            <p class="text-lg font-semibold text-gray-800"><strong>Selected Table:</strong> {{ table_name }}</p>
            <form method="POST" action="/reset" class="inline">
                <button type="submit" class="bg-red-500 text-white px-4 py-2 rounded-md hover:bg-red-600">Reset</button>
            </form>
        </div>
        <div class="mb-6">
            <h3 class="text-xl font-semibold text-gray-800 mb-2">Table Metadata</h3>
            <div class="flex items-center mb-4">
                <input id="column-search" type="text" placeholder="Search columns..." class="w-full p-2 border rounded-md">
                <button onclick="searchColumns()" class="ml-2 bg-blue-500 text-white px-4 py-2 rounded-md hover:bg-blue-600">Search</button>
            </div>
            <table class="w-full border-collapse border">
                <thead>
                    <tr class="bg-gray-200">
                        <th class="border p-2">Column</th>
                        <th class="border p-2">Type</th>
                        <th class="border p-2">Description</th>
                    </tr>
                </thead>
                <tbody id="column-table">
                    {% for col in metadata %}
                    <tr class="column-row">
                        <td class="border p-2">{{ col.column_name }}</td>
                        <td class="border p-2">{{ col.type }}</td>
                        <td class="border p-2">{{ col.description }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        <div class="mb-6">
            <h3 class="text-xl font-semibold text-gray-800 mb-2">Suggested Questions</h3>
            <ul class="space-y-2">
                {% for suggestion in suggestions %}
                <li class="suggestion p-2 rounded-md bg-gray-100" onclick="fillQuery({{ suggestion|tojson }})">{{ suggestion }}</li>
                {% endfor %}
            </ul>
        </div>
        <div class="mb-6">
            <form method="POST" action="/generate_response" class="space-y-4">
                <label for="query" class="block text-sm font-medium text-gray-700">Enter your query or type 'explain table'</label>
                <textarea id="query" name="query" required class="w-full p-2 border rounded-md"></textarea>
                <button type="submit" class="bg-green-500 text-white px-4 py-2 rounded-md hover:bg-green-600">Generate Response</button>
            </form>
        </div>
        {% if explanation %}
        <h2 class="text-xl font-semibold text-gray-800 mb-2">Table Explanation</h2>
        <div class="p-4 bg-gray-50 rounded-md">{{ explanation }}</div>
        {% endif %}
        {% if sas_code %}
        <h2 class="text-xl font-semibold text-gray-800 mb-2">Generated SAS PROC SQL Code</h2>
        <pre class="p-4 bg-gray-50 rounded-md overflow-x-auto">{{ sas_code }}</pre>
        <form method="GET" action="/download">
            <button type="submit" class="bg-blue-500 text-white px-4 py-2 rounded-md hover:bg-blue-600">Download SAS File</button>
        </form>
        {% endif %}
        {% if error %}
        <p class="text-red-500 mt-4">{{ error }}</p>
        {% endif %}
        {% endif %}
        {% if success %}
        <p class="text-green-500 mt-4">{{ success }}</p>
        {% endif %}
    </div>
    <script>
        function searchColumns() {
            const input = document.getElementById('column-search').value.toLowerCase();
            const rows = document.querySelectorAll('#column-table .column-row');
            rows.forEach((row, index) => {
                const columnName = row.cells[0].textContent.toLowerCase();
                row.classList.toggle('visible', columnName.includes(input) || (input === '' && index < 5));
            });
        }

        function fillQuery(query) {
            document.getElementById('query').value = query;
        }

        document.addEventListener('DOMContentLoaded', () => {
            const rows = document.querySelectorAll('#column-table .column-row');
            rows.forEach((row, index) => {
                if (index < 5) row.classList.add('visible');
            });
            document.getElementById('column-search').addEventListener('input', searchColumns);
        });
    </script>
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

def call_gemini_api(prompt, max_attempts=3, initial_delay=1):
    """Call Gemini API with exponential backoff for 429 errors."""
    model = genai.GenerativeModel(MODEL_NAME)
    
    for attempt in range(max_attempts):
        try:
            response = model.generate_content(prompt)
            output = response.text.strip()
            logger.info(f"API call successful: {output[:50]}...")
            return output
        except HTTPError as e:
            if e.response.status_code == 429:
                retry_after = initial_delay * (2 ** attempt)
                logger.warning(f"429 Too Many Requests. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            logger.error(f"Error calling Gemini API (attempt {attempt + 1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(initial_delay * (2 ** attempt))
        except Exception as e:
            logger.error(f"Error calling Gemini API (attempt {attempt + 1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(initial_delay * (2 ** attempt))
    return "Error processing request: Too many attempts or rate limit exceeded."

def explain_table(table_name):
    """Generate an explanation of the table using Gemini API, with caching."""
    if table_name in explanation_cache:
        logger.info(f"Using cached explanation for {table_name}")
        return explanation_cache[table_name]
    
    metadata = get_table_metadata(table_name)
    columns_info = "\n".join([f"- {col['column_name']}: {col['type']} ({col['description']})" for col in metadata])
    prompt = f"""
    You are an expert in database analysis. Based on the table name and its column metadata, provide a concise explanation of the table's purpose and structure. Focus on the table's role in a business or system context, inferred from the table name and column names/types/descriptions. Do not include any information unrelated to the table or its metadata. Return only the explanation text.

    Table Name: {table_name}
    Columns:
    {columns_info}
    """
    explanation = call_gemini_api(prompt)
    if explanation != "Error processing request: Too many attempts or rate limit exceeded.":
        explanation_cache[table_name] = explanation
    return explanation

def generate_sas_query(query, table_name):
    """Convert natural language query to SAS PROC SQL using Gemini API."""
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
    - Return only the SAS PROC SQL code or the error message, without additional text or unrelated content.

    Query: {query}
    """
    return call_gemini_api(prompt)

def generate_suggestions(table_name):
    """Generate 5 relevant suggested questions for the table using Gemini API."""
    if table_name in suggestions_cache:
        logger.info(f"Using cached suggestions for {table_name}")
        return suggestions_cache[table_name]
    
    metadata = get_table_metadata(table_name)
    columns_info = "\n".join([f"- {col['column_name']}: {col['type']} ({col['description']})" for col in metadata])
    prompt = f"""
    You are an expert in database analysis. Based on the table name and its column metadata, generate exactly 5 concise, relevant questions that users might ask to query the table in simple English. Each question should be directly related to the table's columns and purpose (e.g., filtering, aggregating, or joining data). Do not include any information unrelated to the table or its metadata. Return the questions as a numbered list in plain text.

    Table Name: {table_name}
    Columns:
    {columns_info}

    Example format:
    1. List all employees with salary greater than 50000
    2. Show the total sales amount by region
    3. Find customers who joined after 2023
    4. Count the number of products in each category
    5. Retrieve orders placed in the last month
    """
    suggestions_text = call_gemini_api(prompt)
    if suggestions_text == "Error processing request: Too many attempts or rate limit exceeded.":
        return []
    suggestions = [line.strip()[3:] for line in suggestions_text.split("\n") if line.strip().startswith(tuple("12345"))]
    if len(suggestions) == 5:
        suggestions_cache[table_name] = suggestions
    return suggestions

def save_sas_file(sas_code):
    """Save SAS code to a file and return the filename."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sas_query_{timestamp}.sas"
    with open(filename, "w") as f:
        f.write(sas_code)
    return filename

def start_ngrok_with_retry(max_attempts=3, delay=5):
    """Start ngrok with retry mechanism to handle ERR_NGROK_3200."""
    for attempt in range(max_attempts):
        try:
            subprocess.run(["pkill", "ngrok"], check=False)
            public_url = ngrok.connect(5000).public_url
            return public_url
        except Exception as e:
            logger.error(f"Ngrok attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                time.sleep(delay)
    raise Exception("Failed to start ngrok after multiple attempts. Please check your ngrok token and internet connection.")

@app.route("/", methods=["GET"])
def index():
    global table_name
    tables = get_tables()
    try:
        return render_template_string(HTML_TEMPLATE, table_name=table_name, tables=tables)
    except Exception as e:
        logger.error(f"Template rendering error: {e}")
        return f"Error rendering template: {str(e)}", 500

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
    suggestions = generate_suggestions(table_name)
    try:
        return render_template_string(
            HTML_TEMPLATE,
            table_name=table_name,
            tables=tables,
            metadata=metadata,
            suggestions=suggestions,
            success=f"Table '{table_name}' selected."
        )
    except Exception as e:
        logger.error(f"Template rendering error: {e}")
        return f"Error rendering template: {str(e)}", 500

@app.route("/reset", methods=["POST"])
def reset():
    global table_name, current_sas_file
    table_name = None
    current_sas_file = None
    tables = get_tables()
    try:
        return render_template_string(
            HTML_TEMPLATE,
            table_name=table_name,
            tables=tables,
            success="Form reset. Please select a table."
        )
    except Exception as e:
        logger.error(f"Template rendering error: {e}")
        return f"Error rendering template: {str(e)}", 500

@app.route("/generate_response", methods=["POST"])
def generate_response():
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
            suggestions=generate_suggestions(table_name),
            error="Query cannot be empty."
        )
    
    query_lower = query.lower()
    is_explanation = "explain table" in query_lower or "describe table" in query_lower or "what is this table" in query_lower
    
    if is_explanation:
        explanation = explain_table(table_name)
        if explanation == "Error processing request: Too many attempts or rate limit exceeded.":
            return render_template_string(
                HTML_TEMPLATE,
                table_name=table_name,
                tables=tables,
                metadata=get_table_metadata(table_name),
                suggestions=generate_suggestions(table_name),
                error="Failed to generate table explanation: API rate limit exceeded. Please wait and try again."
            )
        try:
            return render_template_string(
                HTML_TEMPLATE,
                table_name=table_name,
                tables=tables,
                metadata=get_table_metadata(table_name),
                suggestions=generate_suggestions(table_name),
                explanation=explanation
            )
        except Exception as e:
            logger.error(f"Template rendering error: {e}")
            return f"Error rendering template: {str(e)}", 500
    else:
        sas_code = generate_sas_query(query, table_name)
        if sas_code == "Query cannot be converted to SAS PROC SQL":
            return render_template_string(
                HTML_TEMPLATE,
                table_name=table_name,
                tables=tables,
                metadata=get_table_metadata(table_name),
                suggestions=generate_suggestions(table_name),
                error="Query cannot be converted to SAS PROC SQL."
            )
        if sas_code == "Error processing request: Too many attempts or rate limit exceeded.":
            return render_template_string(
                HTML_TEMPLATE,
                table_name=table_name,
                tables=tables,
                metadata=get_table_metadata(table_name),
                suggestions=generate_suggestions(table_name),
                error="Failed to generate SAS query: API rate limit exceeded. Please wait and try again."
            )
        
        filename = save_sas_file(sas_code)
        global current_sas_file
        current_sas_file = filename
        
        try:
            return render_template_string(
                HTML_TEMPLATE,
                table_name=table_name,
                tables=tables,
                metadata=get_table_metadata(table_name),
                suggestions=generate_suggestions(table_name),
                sas_code=sas_code
            )
        except Exception as e:
            logger.error(f"Template rendering error: {e}")
            return f"Error rendering template: {str(e)}", 500

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
            suggestions=generate_suggestions(table_name) if table_name else [],
            error="No SAS file available for download."
        )
    return send_file(current_sas_file, as_attachment=True, download_name=os.path.basename(current_sas_file))

if __name__ == "__main__":
    try:
        subprocess.run(["wget", "https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-linux-amd64.tgz"], check=True)
        subprocess.run(["tar", "-xvzf", "ngrok-v3-stable-linux-amd64.tgz"], check=True)
        subprocess.run(["mv", "ngrok", "/usr/local/bin/"], check=True)
    except Exception as e:
        logger.error(f"Failed to update ngrok client: {e}")
    
    try:
        ngrok.set_auth_token("your_ngrok_authtoken_here")
        public_url = start_ngrok_with_retry()
        logger.info(f"Flask app running at: {public_url}")
    except Exception as e:
        logger.error(f"Error starting ngrok: {e}")
        exit(1)
    
    logger.info("Running network diagnostics...")
    subprocess.run(["nslookup", "generativelanguage.googleapis.com"])
    subprocess.run(["ping", "-c", "4", "generativelanguage.googleapis.com"])
    
    app.run(port=5000)
