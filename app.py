
import os
import datetime
import hashlib
import json
import random
import base64
import time
from urllib.parse import urlparse, unquote

import numpy as np
import cv2
try:
    import face_recognition
except ModuleNotFoundError:
    face_recognition = None

from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from flask_bcrypt import Bcrypt
import mysql.connector
from functools import wraps
try:
    from flask_mail import Mail, Message
except ModuleNotFoundError:
    Mail = None
    Message = None
from werkzeug.utils import secure_filename

# --- APP CONFIGURATION ---
app = Flask(__name__, static_folder='templates/static')
app.secret_key = os.environ.get("FLASK_SECRET", os.urandom(24))
bcrypt = Bcrypt(app)

# Database Configuration
DB_HOST = os.environ.get("DB_HOST") or os.environ.get("MYSQLHOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT") or os.environ.get("MYSQLPORT", 3306))
DB_USER = os.environ.get("DB_USER") or os.environ.get("MYSQLUSER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD") or os.environ.get("MYSQLPASSWORD", "")
DB_NAME = os.environ.get("DB_NAME") or os.environ.get("MYSQLDATABASE", "evoting_db")


def _load_database_url():
    database_url = (
        os.environ.get("DATABASE_URL")
        or os.environ.get("MYSQL_URL")
        or os.environ.get("MYSQL_PUBLIC_URL")
    )
    if not database_url:
        return

    parsed = urlparse(database_url)
    if not parsed.scheme.startswith("mysql"):
        return

    global DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
    DB_HOST = parsed.hostname or DB_HOST
    DB_PORT = parsed.port or DB_PORT
    DB_USER = unquote(parsed.username) if parsed.username else DB_USER
    DB_PASSWORD = unquote(parsed.password) if parsed.password else DB_PASSWORD
    if parsed.path and parsed.path != "/":
        DB_NAME = parsed.path.lstrip("/")


_load_database_url()

# Email Configuration (Flask-Mail)
app.config['MAIL_SERVER'] = os.environ.get('MAIL_SERVER', 'smtp.gmail.com')
app.config['MAIL_PORT'] = int(os.environ.get('MAIL_PORT', 587))
app.config['MAIL_USE_TLS'] = os.environ.get('MAIL_USE_TLS', 'true').lower() == 'true'
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.environ.get('MAIL_DEFAULT_SENDER') or app.config['MAIL_USERNAME']

mail = Mail(app) if Mail else None

ADMIN_REGISTRATION_CODE = os.environ.get("ADMIN_REGISTRATION_CODE", "ADMIN2025")
FALLBACK_ADMIN_USERNAME = os.environ.get("FALLBACK_ADMIN_USERNAME", "abhishek2511")

# Upload Config
UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', 'templates/static/uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# --- DATABASE MANAGEMENT ---

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return conn
    except mysql.connector.Error as err:
        if err.errno == 1049:
            conn = mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD)
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            conn.commit()
            conn.close()
            return mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        else:
            print(f"Database Connection Error: {err}")
            return None


def init_db():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()

        # Users Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                full_name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role ENUM('admin', 'voter') DEFAULT 'voter',
                face_encoding TEXT,
                otp_code VARCHAR(6),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Ensure new columns exist (for upgrades)
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'users'
        """, (DB_NAME,))
        existing_cols = {row[0] for row in cursor.fetchall()}

        def add_column(col_sql):
            cursor.execute(f"ALTER TABLE users ADD COLUMN {col_sql}")

        if 'unique_id' not in existing_cols:
            add_column("unique_id VARCHAR(50)")
        if 'is_email_verified' not in existing_cols:
            add_column("is_email_verified TINYINT(1) DEFAULT 0")
        if 'approval_status' not in existing_cols:
            add_column("approval_status ENUM('pending','approved','rejected') DEFAULT 'pending'")
        if 'is_approved' not in existing_cols:
            add_column("is_approved TINYINT(1) DEFAULT 0")
        if 'face_image_path' not in existing_cols:
            add_column("face_image_path VARCHAR(255)")
        if 'otp_purpose' not in existing_cols:
            add_column("otp_purpose ENUM('email','login') NULL")
        if 'otp_expires' not in existing_cols:
            add_column("otp_expires DATETIME NULL")
        if 'approved_at' not in existing_cols:
            add_column("approved_at DATETIME NULL")
        if 'org_id' not in existing_cols:
            add_column("org_id INT NULL")
        if 'is_org_admin' not in existing_cols:
            add_column("is_org_admin TINYINT(1) DEFAULT 0")
        if 'org_approval_status' not in existing_cols:
            add_column("org_approval_status ENUM('pending','approved','rejected') DEFAULT 'pending'")
        if 'date_of_birth' not in existing_cols:
            add_column("date_of_birth DATE NULL")
        if 'gov_id' not in existing_cols:
            add_column("gov_id VARCHAR(100) NULL")
        if 'department' not in existing_cols:
            add_column("department VARCHAR(150) NULL")
        if 'voter_status' not in existing_cols:
            add_column("voter_status ENUM('active','suspended') DEFAULT 'active'")
        if 'approved_by' not in existing_cols:
            add_column("approved_by INT NULL")
        if 'rejected_at' not in existing_cols:
            add_column("rejected_at DATETIME NULL")

        # Org Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orgs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                org_name VARCHAR(150) NOT NULL,
                org_code VARCHAR(50) UNIQUE NOT NULL,
                created_by INT,
                approval_status ENUM('pending','approved','rejected') DEFAULT 'approved',
                org_logo_path VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Elections Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS elections (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                description TEXT,
                start_date DATETIME,
                end_date DATETIME,
                status ENUM('upcoming', 'ongoing', 'completed') DEFAULT 'upcoming'
            )
        """)

        # Ensure org_id on elections
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'elections'
        """, (DB_NAME,))
        election_cols = {row[0] for row in cursor.fetchall()}
        if 'org_id' not in election_cols:
            cursor.execute("ALTER TABLE elections ADD COLUMN org_id INT")
        # Candidates Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id INT AUTO_INCREMENT PRIMARY KEY,
                election_id INT,
                name VARCHAR(100),
                description TEXT,
                party VARCHAR(100),
                photo_url VARCHAR(255),
                vote_count INT DEFAULT 0,
                FOREIGN KEY (election_id) REFERENCES elections(id) ON DELETE CASCADE
            )
        """)

        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'candidates'
        """, (DB_NAME,))
        candidate_cols = {row[0] for row in cursor.fetchall()}
        if 'description' not in candidate_cols:
            cursor.execute("ALTER TABLE candidates ADD COLUMN description TEXT")

        # Blockchain Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS blockchain (
                id INT AUTO_INCREMENT PRIMARY KEY,
                election_id INT,
                voter_hash VARCHAR(64),
                candidate_id INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                previous_hash VARCHAR(64),
                current_hash VARCHAR(64),
                FOREIGN KEY (election_id) REFERENCES elections(id) ON DELETE CASCADE
            )
        """)

        # Plain vote records
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                voter_id INT NOT NULL,
                election_id INT NOT NULL,
                candidate_id INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uniq_voter_election (voter_id, election_id),
                FOREIGN KEY (election_id) REFERENCES elections(id) ON DELETE CASCADE,
                FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
                FOREIGN KEY (voter_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)

        # Election voter assignments
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS election_voters (
                id INT AUTO_INCREMENT PRIMARY KEY,
                election_id INT NOT NULL,
                voter_id INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uniq_election_voter (election_id, voter_id),
                FOREIGN KEY (election_id) REFERENCES elections(id) ON DELETE CASCADE,
                FOREIGN KEY (voter_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)

        # Audit logs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                actor_user_id INT NULL,
                actor_role VARCHAR(20) NULL,
                action_type VARCHAR(50) NOT NULL,
                target_type VARCHAR(50) NULL,
                target_id INT NULL,
                details TEXT NULL,
                ip_address VARCHAR(45) NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Database initialized successfully.")


init_db()


# Ensure a default admin exists

def ensure_default_admin():
    admin_email = os.environ.get("ADMIN_EMAIL")
    admin_password = os.environ.get("ADMIN_PASSWORD")
    admin_name = os.environ.get("ADMIN_NAME", "Platform Admin")
    admin_unique_id = os.environ.get("ADMIN_UNIQUE_ID", "abhishek2511")

    if not admin_email or not admin_password:
        print("Skipping default admin creation because ADMIN_EMAIL or ADMIN_PASSWORD is not set.")
        return

    conn = get_db_connection()
    if not conn:
        print("Skipping default admin creation because database connection is unavailable.")
        return
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id FROM users WHERE role = 'admin' AND email = %s", (admin_email,))
    admin = cursor.fetchone()
    if not admin:
        pw_hash = bcrypt.generate_password_hash(admin_password).decode('utf-8')
        cursor.execute("""
            INSERT INTO users (full_name, email, password_hash, role, unique_id, is_email_verified, approval_status)
            VALUES (%s, %s, %s, 'admin', %s, 1, 'approved')
        """, (admin_name, admin_email, pw_hash, admin_unique_id))
        conn.commit()
        print(f"Default admin created: {admin_email} / {admin_password}")
    cursor.close()
    conn.close()

ensure_default_admin()


def log_audit_event(action_type, target_type=None, target_id=None, details=None, actor_user_id=None, actor_role=None):
    try:
        conn = get_db_connection()
        if not conn:
            return
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO audit_logs (actor_user_id, actor_role, action_type, target_type, target_id, details, ip_address)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            actor_user_id if actor_user_id is not None else session.get('user_id'),
            actor_role if actor_role is not None else session.get('role'),
            action_type,
            target_type,
            target_id,
            json.dumps(details) if isinstance(details, (dict, list)) else details,
            request.headers.get('X-Forwarded-For', request.remote_addr) if request else None
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Audit log error: {e}")


def get_org_admin_approver(org_id):
    if not org_id:
        return None

    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, full_name, email, unique_id
        FROM users
        WHERE role = 'admin'
          AND is_org_admin = 1
          AND org_id = %s
          AND approval_status = 'approved'
          AND org_approval_status = 'approved'
        ORDER BY id ASC
        LIMIT 1
    """, (org_id,))
    approver = cursor.fetchone()
    cursor.close()
    conn.close()
    return approver


def get_fallback_admin_approver():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, full_name, email, unique_id
        FROM users
        WHERE role = 'admin'
          AND approval_status = 'approved'
          AND (unique_id = %s OR email = %s)
        ORDER BY id ASC
        LIMIT 1
    """, (FALLBACK_ADMIN_USERNAME, os.environ.get("ADMIN_EMAIL", "")))
    approver = cursor.fetchone()
    if not approver:
        cursor.execute("""
            SELECT id, full_name, email, unique_id
            FROM users
            WHERE role = 'admin' AND approval_status = 'approved'
            ORDER BY id ASC
            LIMIT 1
        """)
        approver = cursor.fetchone()
    cursor.close()
    conn.close()
    return approver


def resolve_voter_approval_route(org_id):
    org_admin = get_org_admin_approver(org_id)
    if org_admin:
        return {
            "approval_status": "approved",
            "org_approval_status": "pending",
            "pending_with": "organization_admin",
            "approver": org_admin,
        }

    fallback_admin = get_fallback_admin_approver()
    return {
        "approval_status": "pending",
        "org_approval_status": "approved",
        "pending_with": "global_admin",
        "approver": fallback_admin,
    }


def get_user_review_status(user):
    if user.get('org_approval_status') == 'rejected' or user.get('approval_status') == 'rejected':
        return 'Rejected'
    if user.get('org_approval_status') == 'pending':
        return 'Pending Organization Approval'
    if user.get('approval_status') == 'pending':
        return 'Pending Admin Approval'
    if user.get('org_approval_status') == 'approved' and user.get('approval_status') == 'approved':
        return 'Approved'
    return 'Pending'


def build_pending_user_state(user_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT approval_status, org_approval_status, created_at, email, org_id
        FROM users
        WHERE id = %s
    """, (user_id,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if not user:
        return None

    if user.get('org_approval_status') == 'pending':
        approver = get_org_admin_approver(user.get('org_id'))
        pending_with = 'organization admin'
    elif user.get('approval_status') == 'pending':
        approver = get_fallback_admin_approver()
        pending_with = 'global admin'
    else:
        approver = None
        pending_with = None

    review_status = get_user_review_status(user)
    if review_status == 'Approved':
        status_message = 'Your account is approved. You can log in now.'
    elif review_status == 'Rejected':
        status_message = 'Your registration was rejected. Please contact support.'
    elif pending_with == 'organization admin':
        approver_name = approver.get('full_name') if approver else None
        status_message = f"Thank you for registering. Your application is under review by your organization admin{f' ({approver_name})' if approver_name else ''}."
    elif pending_with == 'global admin':
        approver_name = approver.get('full_name') if approver else None
        status_message = f"Thank you for registering. No organization admin is available for your Org ID, so your approval request has been sent to admin abhishek2511{f' ({approver_name})' if approver_name else ''}."
    else:
        status_message = 'Thank you for registering. Your application is under review.'

    return {
        **user,
        'pending_with': pending_with,
        'approver_name': approver.get('full_name') if approver else None,
        'review_status': review_status,
        'status_message': status_message,
    }


# --- HELPER FUNCTIONS ---

def send_otp_email(to_email, otp):
    try:
        if mail is None or Message is None:
            print("Flask-Mail is not installed. OTP email could not be sent.")
            return False
        if not app.config.get('MAIL_USERNAME') or not app.config.get('MAIL_PASSWORD'):
            print("Mail credentials are missing. OTP email could not be sent.")
            return False
        msg = Message("OTP Verification", recipients=[to_email])
        msg.body = f"Your OTP is {otp}"
        mail.send(msg)
        print(f"Email sent successfully to {to_email}")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False


def process_face_image(image_path):
    try:
        if face_recognition is None:
            print("Face recognition dependency is not installed.")
            return None
        image = face_recognition.load_image_file(image_path)
        encodings = face_recognition.face_encodings(image)
        if len(encodings) > 0:
            return json.dumps(encodings[0].tolist())
        return None
    except Exception as e:
        print(f"Face Processing Error: {e}")
        return None


def process_face_image_b64(image_b64):
    try:
        if face_recognition is None:
            print("Face recognition dependency is not installed.")
            return None, None
        if ',' in image_b64:
            encoded_data = image_b64.split(',')[1]
        else:
            encoded_data = image_b64
        nparr = np.frombuffer(base64.b64decode(encoded_data), np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return None, None
        rgb_img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        encodings = face_recognition.face_encodings(rgb_img)
        if len(encodings) > 0:
            filename = f"face_{int(time.time())}_{random.randint(1000,9999)}.jpg"
            path = os.path.join(UPLOAD_FOLDER, filename)
            cv2.imwrite(path, img)
            return json.dumps(encodings[0].tolist()), f"uploads/{filename}"
        return None, None
    except Exception as e:
        print(f"Face Processing Error (b64): {e}")
        return None, None


def verify_face_match(stored_encoding_json, current_image_b64):
    try:
        if face_recognition is None:
            print("Face recognition dependency is not installed.")
            return False
        if not stored_encoding_json:
            return False

        known_encoding = np.array(json.loads(stored_encoding_json))

        if ',' in current_image_b64:
            encoded_data = current_image_b64.split(',')[1]
        else:
            encoded_data = current_image_b64

        nparr = np.frombuffer(base64.b64decode(encoded_data), np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        rgb_img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        unknown_encodings = face_recognition.face_encodings(rgb_img)

        if len(unknown_encodings) > 0:
            matches = face_recognition.compare_faces([known_encoding], unknown_encodings[0], tolerance=0.5)
            return matches[0]
        return False
    except Exception as e:
        print(f"Face Match Error: {e}")
        return False




def is_duplicate_face(new_encoding_json, org_id=None, tolerance=0.45):
    try:
        if face_recognition is None:
            return False
        if not new_encoding_json:
            return False
        new_enc = np.array(json.loads(new_encoding_json))
        conn = get_db_connection()
        if not conn:
            return False
        cursor = conn.cursor(dictionary=True)
        if org_id:
            cursor.execute("SELECT face_encoding FROM users WHERE face_encoding IS NOT NULL AND org_id = %s", (org_id,))
        else:
            cursor.execute("SELECT face_encoding FROM users WHERE face_encoding IS NOT NULL")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        for r in rows:
            try:
                known = np.array(json.loads(r['face_encoding']))
                matches = face_recognition.compare_faces([known], new_enc, tolerance=tolerance)
                if matches and matches[0]:
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False

# --- DECORATORS ---

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash("Please log in to access this page.", "danger")
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'role' not in session or session['role'] != 'admin':
            flash("Access denied. Admin privileges required.", "danger")
            return redirect(url_for('voter_dashboard'))
        return f(*args, **kwargs)
    return decorated_function


# --- ROUTES ---

@app.route('/')
@app.route('/index.html')
def index():
    return render_template('index.html')


@app.route('/privacy.html')
def privacy():
    return render_template('privacy.html')


@app.route('/registration_page.html', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        full_name = request.form.get('full_name', '').strip()
        email = request.form.get('email', '').strip()
        unique_id = request.form.get('unique_id', '').strip()
        if not unique_id:
            base = ''.join(ch for ch in full_name.lower() if ch.isalnum() or ch == ' ')
            base = '_'.join(base.split())[:20]
            unique_id = f"{base}_{random.randint(100,999)}" if base else f"user_{random.randint(1000,9999)}"
        password = request.form.get('password', '').strip()
        confirm_password = request.form.get('confirm_password', '').strip()
        requested_role = request.form.get('role', 'voter')
        org_code = request.form.get('org_code', '').strip()
        org_name = request.form.get('org_name', '').strip()
        date_of_birth = request.form.get('date_of_birth', '').strip()
        gov_id = request.form.get('gov_id', '').strip()
        department = request.form.get('department', '').strip()

        if not org_code:
            flash('Organization ID is required.', 'danger')
            return redirect(request.url)
        if not full_name or not email or not unique_id or not password or not date_of_birth or not gov_id or not department:
            flash('All fields are required.', 'danger')
            return redirect(request.url)

        if not full_name or not email or not unique_id or not password:
            flash('All fields are required.', 'danger')
            return redirect(request.url)
        if password != confirm_password:
            flash('Passwords do not match.', 'danger')
            return redirect(request.url)

        face_encoding = None
        face_image_path = None

        file = request.files.get('face_image')
        if file and file.filename:
            safe_name = secure_filename(file.filename)
            ext = os.path.splitext(safe_name)[1] or '.jpg'
            filename = f"face_{int(time.time())}_{random.randint(1000,9999)}{ext}"
            save_path = os.path.join(UPLOAD_FOLDER, filename)
            file.save(save_path)
            face_encoding = process_face_image(save_path)
            face_image_path = f"uploads/{filename}"
        else:
            image_b64 = request.form.get('face_image_b64')
            if image_b64:
                face_encoding, face_image_path = process_face_image_b64(image_b64)

        if not face_encoding:
            flash('Face not detected. Please use a clear photo.', 'danger')
            return redirect(request.url)

        # Prevent duplicate face registrations (one face only once)
        if is_duplicate_face(face_encoding, org_id=None):
            flash('This face is already registered. Only one account per face is allowed.', 'danger')
            return redirect(request.url)

        pw_hash = bcrypt.generate_password_hash(password).decode('utf-8')
        otp = str(random.randint(100000, 999999))
        otp_expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # handle org creation or lookup
        cursor.execute("SELECT * FROM orgs WHERE org_code = %s", (org_code,))
        org = cursor.fetchone()
        if not org:
            if not org_name:
                flash('Organization name is required for new organization.', 'danger')
                cursor.close()
                conn.close()
                return redirect(request.url)
            cursor.execute("INSERT INTO orgs (org_name, org_code, created_by, approval_status) VALUES (%s, %s, NULL, 'approved')", (org_name, org_code))
            conn.commit()
            cursor.execute("SELECT * FROM orgs WHERE org_code = %s", (org_code,))
            org = cursor.fetchone()

        org_id = org['id']

        # Route voter approval to an approved org admin when available, otherwise
        # fall back to the main admin account.
        is_org_admin = 1 if requested_role == 'admin' else 0
        approval_status = 'approved'
        org_approval_status = 'approved'
        pending_with = None
        if requested_role == 'admin':
            org_approval_status = 'approved'
        else:
            approval_route = resolve_voter_approval_route(org_id)
            approval_status = approval_route['approval_status']
            org_approval_status = approval_route['org_approval_status']
            pending_with = approval_route['pending_with']
        voter_status = 'active'
        otp = str(random.randint(100000, 999999))
        otp_expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)

        cursor.close()
        conn.close()

        conn = get_db_connection()
        if not conn:
            return database_unavailable_response()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO users (full_name, email, password_hash, role, face_encoding, unique_id, is_email_verified,
                                   approval_status, otp_code, otp_purpose, otp_expires, face_image_path, org_id, is_org_admin, org_approval_status, date_of_birth, gov_id, department, voter_status)
                VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s, 'email', %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (full_name, email, pw_hash, requested_role, face_encoding, unique_id, approval_status, otp, otp_expiry, face_image_path, org_id, is_org_admin, org_approval_status, date_of_birth, gov_id, department, voter_status))
            conn.commit()
            send_otp_email(email, otp)
            session['temp_user_id'] = cursor.lastrowid
            session['email'] = email
            session['pending_with'] = pending_with
            flash('Registration successful. Please verify your email with the OTP sent.', 'info')
            return redirect(url_for('email_verification'))
        except mysql.connector.IntegrityError:
            flash('Email already registered.', 'danger')
        finally:
            cursor.close()
            conn.close()

    return render_template('registration_page.html')


@app.route('/login', methods=['POST'])
def login():
    username = request.form.get('username', '').strip() or request.form.get('email_or_id', '').strip()
    password = request.form.get('password', '').strip()

    if not username or not password:
        log_audit_event('login_attempt_failed', target_type='user', details={'username': username, 'reason': 'missing_credentials'}, actor_role='anonymous')
        flash('Username/Email and password are required.', 'danger')
        return redirect(url_for('index'))

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Allow login via email or unique_id (username)
    if '@' in username:
        cursor.execute("SELECT * FROM users WHERE email = %s", (username,))
    else:
        cursor.execute("SELECT * FROM users WHERE unique_id = %s OR email = %s", (username, username))

    user = cursor.fetchone()
    cursor.close()
    conn.close()

    if user and bcrypt.check_password_hash(user['password_hash'], password):
        if user.get('role') != 'admin':
            if not user.get('is_approved'):
                log_audit_event('login_attempt_blocked', target_type='user', target_id=user['id'], details={'username': username, 'reason': 'not_approved'}, actor_user_id=user['id'], actor_role=user.get('role'))
                session['pending_user_id'] = user['id']
                session['email'] = user['email']
                session['org_id'] = user.get('org_id')
                session['is_org_admin'] = user.get('is_org_admin', 0)
                flash('Your account is waiting for admin approval.', 'info')
                return redirect(url_for('pending_approval'))
            if user.get('org_approval_status') == 'pending':
                log_audit_event('login_attempt_blocked', target_type='user', target_id=user['id'], details={'username': username, 'reason': 'org_pending'}, actor_user_id=user['id'], actor_role=user.get('role'))
                session['pending_user_id'] = user['id']
                session['email'] = user['email']
                session['org_id'] = user.get('org_id')
                session['is_org_admin'] = user.get('is_org_admin', 0)
                flash('Your organization admin approval is pending.', 'info')
                return redirect(url_for('pending_approval'))
            if user.get('org_approval_status') == 'rejected':
                log_audit_event('login_attempt_blocked', target_type='user', target_id=user['id'], details={'username': username, 'reason': 'org_rejected'}, actor_user_id=user['id'], actor_role=user.get('role'))
                flash('Your organization admin rejected your request.', 'danger')
                return redirect(url_for('index'))
            if user.get('approval_status') == 'pending':
                log_audit_event('login_attempt_blocked', target_type='user', target_id=user['id'], details={'username': username, 'reason': 'admin_pending'}, actor_user_id=user['id'], actor_role=user.get('role'))
                session['pending_user_id'] = user['id']
                session['email'] = user['email']
                session['org_id'] = user.get('org_id')
                session['is_org_admin'] = user.get('is_org_admin', 0)
                flash('Your account is pending admin approval.', 'info')
                return redirect(url_for('pending_approval'))
            if user.get('approval_status') == 'rejected':
                log_audit_event('login_attempt_blocked', target_type='user', target_id=user['id'], details={'username': username, 'reason': 'admin_rejected'}, actor_user_id=user['id'], actor_role=user.get('role'))
                flash('Your registration was rejected. Please contact support.', 'danger')
                return redirect(url_for('index'))

        otp = str(random.randint(100000, 999999))
        otp_expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE users SET otp_code = %s, otp_purpose = 'login', otp_expires = %s WHERE id = %s
        """, (otp, otp_expiry, user['id']))
        conn.commit()
        cursor.close()
        conn.close()

        email_sent = send_otp_email(user['email'], otp)
        if email_sent:
            flash(f"OTP sent to {user['email']}. Please check your inbox.", 'info')
        else:
            flash('Error sending email. Check server logs or credentials.', 'warning')

        session['temp_user_id'] = user['id']
        session['email'] = user['email']
        session['org_id'] = user.get('org_id')
        session['is_org_admin'] = user.get('is_org_admin', 0)
        log_audit_event('login_attempt_success', target_type='user', target_id=user['id'], details={'username': username, 'otp_required': True}, actor_user_id=user['id'], actor_role=user.get('role'))
        return redirect(url_for('otp_verification'))
    else:
        log_audit_event('login_attempt_failed', target_type='user', details={'username': username, 'reason': 'invalid_credentials'}, actor_role='anonymous')
        flash('Invalid email or password', 'danger')
        return redirect(url_for('index'))


@app.route('/otp_login.html', methods=['GET', 'POST'])
def otp_verification():
    if 'temp_user_id' not in session:
        return redirect(url_for('index'))

    if request.method == 'POST':
        action = request.form.get('action', 'verify')
        if action == 'resend':
            conn = get_db_connection()
            if not conn:
                return database_unavailable_response()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT email FROM users WHERE id = %s", (session['temp_user_id'],))
            user = cursor.fetchone()
            cursor.close()
            conn.close()
            if user:
                otp = str(random.randint(100000, 999999))
                otp_expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)
                conn = get_db_connection()
                if not conn:
                    return database_unavailable_response()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE users SET otp_code = %s, otp_purpose = 'login', otp_expires = %s WHERE id = %s
                """, (otp, otp_expiry, session['temp_user_id']))
                conn.commit()
                cursor.close()
                conn.close()
                send_otp_email(user['email'], otp)
                flash('A new OTP has been sent to your email.', 'info')
            return redirect(url_for('otp_verification'))

        entered_otp = request.form.get('otp', '').strip()

        conn = get_db_connection()
        if not conn:
            return database_unavailable_response()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE id = %s", (session['temp_user_id'],))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user and user['otp_code'] == entered_otp and user.get('otp_purpose') == 'login':
            if user.get('otp_expires') and user['otp_expires'] < datetime.datetime.now():
                flash('OTP expired. Please request a new one.', 'danger')
                return redirect(url_for('otp_verification'))

            # Admins can skip face verification if desired
            if user.get('role') == 'admin' or not user.get('face_encoding'):
                session.pop('temp_user_id', None)
                session['user_id'] = user['id']
                session['role'] = user['role']
                session['name'] = user['full_name']
                session['email'] = user['email']
                session['org_id'] = user.get('org_id')
                session['is_org_admin'] = user.get('is_org_admin', 0)
                conn = get_db_connection()
                if not conn:
                    return database_unavailable_response()
                cursor = conn.cursor()
                cursor.execute("UPDATE users SET otp_code = NULL, otp_purpose = NULL, otp_expires = NULL WHERE id = %s", (user['id'],))
                conn.commit()
                cursor.close()
                conn.close()
                return redirect(url_for('admin_dashboard' if user['role'] == 'admin' else 'voter_dashboard'))

            session['otp_verified'] = True
            return redirect(url_for('face_auth'))
        else:
            flash('Invalid OTP', 'danger')

    return render_template('otp_login.html', email=session.get('email'))


@app.route('/face_auth.html', methods=['GET', 'POST'])
def face_auth():
    if 'temp_user_id' not in session or not session.get('otp_verified'):
        return redirect(url_for('index'))

    if request.method == 'POST':
        image_data = request.form.get('image_data')

        if not image_data:
            flash("No image captured.", "danger")
            return redirect(request.url)

        conn = get_db_connection()
        if not conn:
            return database_unavailable_response()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE id = %s", (session['temp_user_id'],))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if verify_face_match(user['face_encoding'], image_data):
            session.pop('temp_user_id', None)
            session.pop('otp_verified', None)
            session['user_id'] = user['id']
            session['role'] = user['role']
            session['name'] = user['full_name']
            session['email'] = user['email']
            session['org_id'] = user.get('org_id')
            session['is_org_admin'] = user.get('is_org_admin', 0)

            flash(f"Welcome back, {user['full_name']}!", 'success')
            return redirect(url_for('admin_dashboard' if user['role'] == 'admin' else 'voter_dashboard'))
        else:
            flash("Face verification failed.", 'danger')
            return redirect(request.url)

    return render_template('face_auth.html')


# --- ADDITIONAL ROUTES ---

@app.route('/email_verification.html', methods=['GET', 'POST'])
def email_verification():
    if 'temp_user_id' not in session:
        return redirect(url_for('index'))

    if request.method == 'POST':
        action = request.form.get('action', 'verify')
        if action == 'resend':
            otp = str(random.randint(100000, 999999))
            otp_expiry = datetime.datetime.now() + datetime.timedelta(minutes=10)
            conn = get_db_connection()
            if not conn:
                return database_unavailable_response()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE users SET otp_code = %s, otp_purpose = 'email', otp_expires = %s WHERE id = %s
            """, (otp, otp_expiry, session['temp_user_id']))
            conn.commit()
            cursor.close()
            conn.close()
            send_otp_email(session.get('email'), otp)
            flash('A new verification code has been sent.', 'info')
            return redirect(url_for('email_verification'))

        entered_otp = request.form.get('otp', '').strip()
        conn = get_db_connection()
        if not conn:
            return database_unavailable_response()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE id = %s", (session['temp_user_id'],))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user and user['otp_code'] == entered_otp and user.get('otp_purpose') == 'email':
            if user.get('otp_expires') and user['otp_expires'] < datetime.datetime.now():
                flash('OTP expired. Please request a new one.', 'danger')
                return redirect(url_for('email_verification'))
            conn = get_db_connection()
            if not conn:
                return database_unavailable_response()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE users SET otp_code = NULL, otp_purpose = NULL, otp_expires = NULL, is_email_verified = 1 WHERE id = %s
            """, (user['id'],))
            conn.commit()
            cursor.close()
            conn.close()
            session.pop('temp_user_id', None)
            session['pending_user_id'] = user['id']
            flash('Email verified. Your account is pending approval.', 'success')
            return redirect(url_for('pending_approval'))
        else:
            flash('Invalid verification code.', 'danger')

    return render_template('email_verification.html', email=session.get('email'))


@app.route('/pending_approval.html')
def pending_approval():
    user_id = session.get('pending_user_id')
    if not user_id:
        return redirect(url_for('index'))
    user = build_pending_user_state(user_id)
    if not user:
        return redirect(url_for('index'))
    return render_template('pending_approval.html', user=user)


@app.route('/api/pending-approval-status')
def pending_approval_status():
    user_id = session.get('pending_user_id')
    if not user_id:
        return jsonify({"ok": False, "message": "No pending user in session"}), 400
    user = build_pending_user_state(user_id)
    if not user:
        return jsonify({"ok": False, "message": "User not found"}), 404
    return jsonify({
        "ok": True,
        "review_status": user['review_status'],
        "status_message": user['status_message'],
        "pending_with": user['pending_with'],
        "approver_name": user['approver_name'],
        "email": user['email'],
        "created_at": user['created_at'].isoformat() if user.get('created_at') else None
    })


@app.route('/review_vote.html')
def review_vote():
    return render_template('review_vote.html')


# --- ADMIN ROUTES ---

@app.route('/admin_dashboard.html')
@login_required
@admin_required
def admin_dashboard():
    return render_template('admin_dashboard.html')


@app.route('/election_creation.html', methods=['GET', 'POST'])
@login_required
@admin_required
def create_election():
    if request.method == 'POST':
        title = request.form['title']
        description = request.form['description']
        start_date = request.form['start_date']
        end_date = request.form['end_date']

        conn = get_db_connection()
        if not conn:
            return database_unavailable_response()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO elections (title, description, start_date, end_date, status, org_id) VALUES (%s, %s, %s, %s, 'upcoming', %s)",
                       (title, description, start_date, end_date, session.get('org_id')))
        conn.commit()
        cursor.close()
        conn.close()
        flash('Election created!', 'success')
        return redirect(url_for('admin_dashboard'))
    return render_template('election_creation.html')


@app.route('/elections_list.html')
@login_required
@admin_required
def manage_elections():
    conn = get_db_connection()
    if not conn:
        return database_unavailable_response()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM elections WHERE org_id = %s", (session.get('org_id'),))
    elections = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('elections_list.html', elections=elections)


@app.route('/election_monitoring.html')
@login_required
@admin_required
def election_monitoring():
    return render_template('election_monitoring.html')


@app.route('/admin_results.html')
@login_required
@admin_required
def admin_results():
    conn = get_db_connection()
    if not conn:
        return database_unavailable_response()
    cursor = conn.cursor(dictionary=True)
    if session.get('is_org_admin'):
        cursor.execute("""
            SELECT *
            FROM elections
            WHERE org_id = %s
            ORDER BY (status = 'completed') DESC, end_date DESC, id DESC
            LIMIT 1
        """, (session.get('org_id'),))
    elif session.get('org_id'):
        cursor.execute("""
            SELECT *
            FROM elections
            WHERE org_id = %s
            ORDER BY (status = 'completed') DESC, end_date DESC, id DESC
            LIMIT 1
        """, (session.get('org_id'),))
    else:
        cursor.execute("""
            SELECT *
            FROM elections
            ORDER BY (status = 'completed') DESC, end_date DESC, id DESC
            LIMIT 1
        """)
    election = cursor.fetchone()

    results_data = {
        "title": "No election results available",
        "start": "",
        "end": "",
        "ballotsCast": 0,
        "invalid": 0,
        "eligible": 0,
        "winner": None,
        "candidates": []
    }

    if election:
        cursor.execute("""
            SELECT
                c.id,
                c.name,
                c.photo_url,
                c.description,
                COUNT(v.id) AS votes
            FROM candidates c
            LEFT JOIN votes v ON v.candidate_id = c.id
            WHERE c.election_id = %s
            GROUP BY c.id, c.name, c.photo_url, c.description
            ORDER BY votes DESC, c.name ASC
        """, (election['id'],))
        candidates = cursor.fetchall()

        cursor.execute("SELECT COUNT(*) AS total_votes FROM votes WHERE election_id = %s", (election['id'],))
        total_votes = cursor.fetchone()['total_votes']

        cursor.execute("SELECT COUNT(*) AS eligible FROM election_voters WHERE election_id = %s", (election['id'],))
        eligible = cursor.fetchone()['eligible']

        winner = None
        if candidates:
            top_candidate = candidates[0]
            if top_candidate['votes'] > 0:
                winner = {
                    "name": top_candidate['name'],
                    "votes": top_candidate['votes'],
                    "photo": top_candidate.get('photo_url') or ""
                }

        results_data = {
            "title": election['title'],
            "start": election['start_date'].strftime('%b %d, %Y %I:%M %p') if election.get('start_date') else "",
            "end": election['end_date'].strftime('%b %d, %Y %I:%M %p') if election.get('end_date') else "",
            "ballotsCast": total_votes,
            "invalid": 0,
            "eligible": eligible,
            "winner": winner,
            "candidates": [
                {
                    "name": c['name'],
                    "votes": int(c['votes'] or 0),
                    "photo": c.get('photo_url') or f"https://picsum.photos/seed/{c['id']}/64/64"
                }
                for c in candidates
            ]
        }

    cursor.close()
    conn.close()
    return render_template('admin_results.html', results_data=results_data)


@app.route('/voter_management.html')
@login_required
@admin_required
def voter_management():
    conn = get_db_connection()
    if not conn:
        return database_unavailable_response()
    cursor = conn.cursor(dictionary=True)
    if session.get('is_org_admin'):
        cursor.execute("""
            SELECT id, full_name, email, role, unique_id, approval_status, org_approval_status, created_at, face_image_path, org_id
            FROM users
            WHERE role = 'voter' AND org_id = %s
        """, (session.get('org_id'),))
        approval_api_base = '/api/org/users'
        default_filter = 'Pending Organization Approval'
    else:
        cursor.execute("""
            SELECT id, full_name, email, role, unique_id, approval_status, org_approval_status, created_at, face_image_path, org_id
            FROM users
            WHERE role = 'voter'
        """)
        approval_api_base = '/api/admin/voters'
        default_filter = 'Pending Admin Approval'
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    voters = []
    for u in users:
        voters.append({
            "id": str(u["id"]),
            "name": u["full_name"],
            "unique_id": u.get("unique_id") or "",
            "email": u["email"],
            "status": get_user_review_status(u),
            "registration_date": u["created_at"].isoformat() if u.get("created_at") else "",
            "face_image_url": url_for('static', filename=u["face_image_path"]) if u.get("face_image_path") else ""
        })
    return render_template(
        'voter_management.html',
        voters_json=json.dumps(voters),
        approval_api_base=approval_api_base,
        default_filter=default_filter
    )


@app.route('/admin_guide.html')
@login_required
@admin_required
def admin_guide():
    return render_template('admin_guide.html')


# --- VOTER ROUTES ---

@app.route('/voter_dashboard.html')
@login_required
def voter_dashboard():
    conn = get_db_connection()
    if not conn:
        return database_unavailable_response()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT e.*,
               CASE WHEN v.id IS NULL THEN 0 ELSE 1 END AS has_voted
        FROM elections e
        JOIN election_voters ev ON ev.election_id = e.id
        LEFT JOIN votes v ON v.election_id = e.id AND v.voter_id = %s
        WHERE e.status = 'ongoing'
          AND ev.voter_id = %s
        ORDER BY e.start_date ASC
    """, (session.get('user_id'), session.get('user_id')))
    active_elections = cursor.fetchall()

    cursor.execute("""
        SELECT e.*
        FROM elections e
        JOIN election_voters ev ON ev.election_id = e.id
        WHERE e.status = 'completed'
          AND ev.voter_id = %s
        ORDER BY e.end_date DESC
    """, (session.get('user_id'),))
    completed_elections = cursor.fetchall()

    cursor.execute("""
        SELECT e.title, v.created_at AS cast_date
        FROM votes v
        JOIN elections e ON e.id = v.election_id
        WHERE v.voter_id = %s
        ORDER BY v.created_at DESC
    """, (session.get('user_id'),))
    voting_history = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template(
        'voter_dashboard.html',
        active_elections=active_elections,
        completed_elections=completed_elections,
        voting_history=voting_history
    )


@app.route('/ballot_page.html')
@login_required
def ballot_page_redirect():
    flash("Please select an election from the dashboard to vote.", "info")
    return redirect(url_for('voter_dashboard'))


@app.route('/vote/<int:election_id>', methods=['GET', 'POST'])
@login_required
def vote(election_id):
    conn = get_db_connection()
    if not conn:
        return database_unavailable_response()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT e.*
        FROM elections e
        JOIN election_voters ev ON ev.election_id = e.id
        WHERE e.id = %s
          AND e.status = 'ongoing'
          AND ev.voter_id = %s
    """, (election_id, session.get('user_id')))
    election = cursor.fetchone()
    if not election:
        cursor.close()
        conn.close()
        flash("This election is not active or you are not assigned to it.", "danger")
        return redirect(url_for('voter_dashboard'))

    cursor.execute("SELECT id FROM votes WHERE election_id = %s AND voter_id = %s", (election_id, session.get('user_id')))

    if cursor.fetchone():
        flash("You have already voted.", "warning")
        cursor.close()
        conn.close()
        return redirect(url_for('voter_dashboard'))

    cursor.execute("SELECT * FROM candidates WHERE election_id = %s", (election_id,))
    candidates = cursor.fetchall()
    cursor.close()
    conn.close()

    return render_template('ballot_page.html', candidates=candidates, election=election)


@app.route('/voter_results.html')
@login_required
def voter_results():
    conn = get_db_connection()
    if not conn:
        return database_unavailable_response()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT e.title, c.name, c.vote_count
        FROM candidates c
        JOIN elections e ON c.election_id = e.id
        WHERE e.status = 'completed'
        ORDER BY e.id, c.vote_count DESC
    """)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('voter_results.html', results=results)


@app.route('/vote_confirmation.html')
@login_required
def vote_confirmation():
    return render_template('vote_confirmation.html')


@app.route('/api/votes', methods=['POST'])
@login_required
def api_cast_vote():
    data = request.get_json(silent=True) or {}
    election_id = data.get('election_id')
    candidate_id = data.get('candidate_id')
    image_data = data.get('image_data')

    try:
        election_id = int(election_id)
        candidate_id = int(candidate_id)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "Invalid election or candidate id."}), 400
    if not image_data:
        return jsonify({"ok": False, "message": "Face verification is required before voting."}), 400

    conn = get_db_connection()
    if not conn:
        return database_unavailable_response(api=True)
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT e.*
        FROM elections e
        JOIN election_voters ev ON ev.election_id = e.id
        WHERE e.id = %s
          AND e.status = 'ongoing'
          AND ev.voter_id = %s
    """, (election_id, session.get('user_id')))
    election = cursor.fetchone()
    if not election:
        cursor.close()
        conn.close()
        return jsonify({"ok": False, "message": "Election is not active or not assigned to you."}), 403

    cursor.execute("SELECT id FROM votes WHERE election_id = %s AND voter_id = %s", (election_id, session.get('user_id')))
    if cursor.fetchone():
        cursor.close()
        conn.close()
        return jsonify({"ok": False, "message": "You have already voted in this election."}), 409

    cursor.execute("SELECT id FROM candidates WHERE id = %s AND election_id = %s", (candidate_id, election_id))
    candidate = cursor.fetchone()
    if not candidate:
        cursor.close()
        conn.close()
        return jsonify({"ok": False, "message": "Selected candidate is invalid."}), 400

    cursor.execute("SELECT face_encoding FROM users WHERE id = %s", (session.get('user_id'),))
    voter = cursor.fetchone()
    if not voter or not voter.get('face_encoding'):
        cursor.close()
        conn.close()
        return jsonify({"ok": False, "message": "No registered face data found for this voter."}), 400
    if not verify_face_match(voter['face_encoding'], image_data):
        cursor.close()
        conn.close()
        log_audit_event('vote_face_verification_failed', target_type='election', target_id=election_id, details={'candidate_id': candidate_id})
        return jsonify({"ok": False, "message": "Face verification failed. Vote not recorded."}), 403

    write_cursor = conn.cursor()
    write_cursor.execute("""
        INSERT INTO votes (voter_id, election_id, candidate_id)
        VALUES (%s, %s, %s)
    """, (session.get('user_id'), election_id, candidate_id))

    user_hash = hashlib.sha256(str(session['user_id']).encode()).hexdigest()
    write_cursor.execute("SELECT current_hash FROM blockchain ORDER BY id DESC LIMIT 1")
    last_block = write_cursor.fetchone()
    prev_hash = last_block[0] if last_block else "0" * 64
    timestamp = str(datetime.datetime.now())
    block_data = f"{election_id}{candidate_id}{user_hash}{timestamp}"
    current_hash = hashlib.sha256((prev_hash + block_data).encode()).hexdigest()

    write_cursor.execute("""
        INSERT INTO blockchain (election_id, voter_hash, candidate_id, previous_hash, current_hash)
        VALUES (%s, %s, %s, %s, %s)
    """, (election_id, user_hash, candidate_id, prev_hash, current_hash))

    write_cursor.execute("UPDATE candidates SET vote_count = vote_count + 1 WHERE id = %s", (candidate_id,))
    conn.commit()
    write_cursor.close()
    cursor.close()
    conn.close()
    log_audit_event('vote_cast', target_type='election', target_id=election_id, details={'candidate_id': candidate_id})
    return jsonify({"ok": True})


@app.route('/logout')
def logout():
    if session.get('user_id'):
        log_audit_event('logout', target_type='user', target_id=session.get('user_id'))
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('index'))


# --- ADMIN API ROUTES ---

@app.route('/api/admin/voters')
@login_required
@admin_required
def api_admin_voters():
    conn = get_db_connection()
    if not conn:
        return database_unavailable_response(api=True)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, full_name, email, unique_id, approval_status, org_approval_status, created_at, face_image_path
        FROM users WHERE role = 'voter'
    """)
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    payload = []
    for u in users:
        payload.append({
            "id": str(u["id"]),
            "name": u["full_name"],
            "unique_id": u.get("unique_id") or "",
            "email": u["email"],
            "status": get_user_review_status(u),
            "registration_date": u["created_at"].isoformat() if u.get("created_at") else "",
            "face_image_url": url_for('static', filename=u["face_image_path"]) if u.get("face_image_path") else ""
        })
    return jsonify(payload)


@app.route('/api/admin/approved-voters')
@login_required
@admin_required
def api_admin_approved_voters():
    conn = get_db_connection()
    if not conn:
        return database_unavailable_response(api=True)
    cursor = conn.cursor(dictionary=True)
    if session.get('is_org_admin'):
        cursor.execute("""
            SELECT id, full_name, unique_id
            FROM users
            WHERE role = 'voter'
              AND org_id = %s
              AND is_approved = 1
            ORDER BY full_name ASC
        """, (session.get('org_id'),))
    else:
        cursor.execute("""
            SELECT id, full_name, unique_id
            FROM users
            WHERE role = 'voter'
              AND is_approved = 1
            ORDER BY full_name ASC
        """)
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([
        {
            "id": u["id"],
            "name": u["full_name"],
            "unique_id": u.get("unique_id") or ""
        }
        for u in users
    ])


@app.route('/api/admin/elections', methods=['POST'])
@login_required
@admin_required
def api_admin_create_election():
    data = request.get_json(silent=True) or {}
    title = (data.get('title') or '').strip()
    description = (data.get('description') or '').strip()
    start_date = data.get('startDate')
    end_date = data.get('endDate')
    candidates = data.get('candidates') or []
    voters = data.get('voters') or []

    if not title or not start_date or not end_date:
        return jsonify({"ok": False, "message": "Title, start date, and end date are required."}), 400
    if len(candidates) < 2:
        return jsonify({"ok": False, "message": "At least 2 candidates are required."}), 400
    if len(voters) < 1:
        return jsonify({"ok": False, "message": "Select at least 1 approved voter."}), 400

    cleaned_candidates = []
    for candidate in candidates:
        name = (candidate.get('name') or '').strip()
        candidate_description = (candidate.get('description') or '').strip()
        photo_url = (candidate.get('photo') or '').strip()
        if not name:
            return jsonify({"ok": False, "message": "Candidate name is required."}), 400
        cleaned_candidates.append({
            "name": name,
            "description": candidate_description,
            "photo_url": photo_url
        })

    voter_ids = []
    for voter_id in voters:
        try:
            voter_ids.append(int(voter_id))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "message": "Invalid voter selection."}), 400

    conn = get_db_connection()
    if not conn:
        return database_unavailable_response(api=True)
    cursor = conn.cursor(dictionary=True)

    if session.get('is_org_admin'):
        cursor.execute(f"""
            SELECT id
            FROM users
            WHERE role = 'voter'
              AND is_approved = 1
              AND org_id = %s
              AND id IN ({','.join(['%s'] * len(voter_ids))})
        """, [session.get('org_id')] + voter_ids)
    else:
        cursor.execute(f"""
            SELECT id
            FROM users
            WHERE role = 'voter'
              AND is_approved = 1
              AND id IN ({','.join(['%s'] * len(voter_ids))})
        """, voter_ids)
    approved_voter_ids = {row['id'] for row in cursor.fetchall()}
    if len(approved_voter_ids) != len(set(voter_ids)):
        cursor.close()
        conn.close()
        return jsonify({"ok": False, "message": "One or more selected voters are not approved."}), 400
    cursor.close()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO elections (title, description, start_date, end_date, status, org_id)
        VALUES (%s, %s, %s, %s, 'upcoming', %s)
    """, (title, description, start_date, end_date, session.get('org_id')))
    election_id = cursor.lastrowid

    for candidate in cleaned_candidates:
        cursor.execute("""
            INSERT INTO candidates (election_id, name, description, party, photo_url)
            VALUES (%s, %s, %s, %s, %s)
        """, (election_id, candidate["name"], candidate["description"], '', candidate["photo_url"]))

    for voter_id in set(voter_ids):
        cursor.execute("""
            INSERT INTO election_voters (election_id, voter_id)
            VALUES (%s, %s)
        """, (election_id, voter_id))

    conn.commit()
    cursor.close()
    conn.close()
    log_audit_event('admin_create_election', target_type='election', target_id=election_id, details={'title': title, 'candidate_count': len(cleaned_candidates), 'assigned_voter_count': len(set(voter_ids))})
    return jsonify({"ok": True, "election_id": election_id})


@app.route('/api/admin/voters/approve', methods=['POST'])
@login_required
@admin_required
def api_admin_voters_approve():
    data = request.get_json(silent=True) or {}
    ids = data.get('ids', [])
    if not ids:
        return jsonify({"ok": False, "message": "No ids"}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE users SET approval_status='approved', org_approval_status='approved', is_approved=1, approved_at=NOW(), approved_by=%s WHERE id IN ({','.join(['%s']*len(ids))})", [session.get('user_id')] + ids)
    conn.commit()
    cursor.close()
    conn.close()
    log_audit_event('admin_approve_voters', target_type='user', details={'approved_ids': ids, 'count': len(ids)})
    return jsonify({"ok": True, "updated": len(ids)})


@app.route('/api/admin/voters/reject', methods=['POST'])
@login_required
@admin_required
def api_admin_voters_reject():
    data = request.get_json(silent=True) or {}
    ids = data.get('ids', [])
    if not ids:
        return jsonify({"ok": False, "message": "No ids"}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE users SET approval_status='rejected', org_approval_status='rejected', is_approved=0, rejected_at=NOW() WHERE id IN ({','.join(['%s']*len(ids))})", ids)
    conn.commit()
    cursor.close()
    conn.close()
    log_audit_event('admin_reject_voters', target_type='user', details={'rejected_ids': ids, 'count': len(ids)})
    return jsonify({"ok": True, "updated": len(ids)})



# --- ORG ADMIN ROUTES (Global Admin) ---
@app.route('/api/admin/orgs')
@login_required
@admin_required
def api_admin_orgs():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM orgs")
    orgs = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(orgs)

@app.route('/api/admin/orgs/approve', methods=['POST'])
@login_required
@admin_required
def api_admin_orgs_approve():
    data = request.get_json(silent=True) or {}
    ids = data.get('ids', [])
    if not ids:
        return jsonify({"ok": False, "message": "No ids"}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE orgs SET approval_status='approved' WHERE id IN ({','.join(['%s']*len(ids))})", ids)
    conn.commit()
    cursor.close()
    conn.close()

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE users SET org_approval_status='approved' WHERE is_org_admin = 1 AND org_id IN ({','.join(['%s']*len(ids))})", ids)
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"ok": True, "updated": len(ids)})

@app.route('/api/admin/orgs/reject', methods=['POST'])
@login_required
@admin_required
def api_admin_orgs_reject():
    data = request.get_json(silent=True) or {}
    ids = data.get('ids', [])
    if not ids:
        return jsonify({"ok": False, "message": "No ids"}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE orgs SET approval_status='rejected' WHERE id IN ({','.join(['%s']*len(ids))})", ids)
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"ok": True, "updated": len(ids)})


# --- ORG ADMIN USER APPROVAL ---
@app.route('/api/org/users/approve', methods=['POST'])
@login_required
def api_org_users_approve():
    if not session.get('is_org_admin'):
        return jsonify({"ok": False, "message": "Forbidden"}), 403
    data = request.get_json(silent=True) or {}
    ids = data.get('ids', [])
    if not ids:
        return jsonify({"ok": False, "message": "No ids"}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE users SET org_approval_status='approved', is_approved=1, approved_by=%s WHERE id IN ({','.join(['%s']*len(ids))}) AND org_id = %s", [session.get('user_id')] + ids + [session.get('org_id')])
    conn.commit()
    cursor.close()
    conn.close()
    log_audit_event('org_admin_approve_voters', target_type='user', details={'approved_ids': ids, 'count': len(ids), 'org_id': session.get('org_id')})
    return jsonify({"ok": True, "updated": len(ids)})

@app.route('/api/org/users/reject', methods=['POST'])
@login_required
def api_org_users_reject():
    if not session.get('is_org_admin'):
        return jsonify({"ok": False, "message": "Forbidden"}), 403
    data = request.get_json(silent=True) or {}
    ids = data.get('ids', [])
    if not ids:
        return jsonify({"ok": False, "message": "No ids"}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE users SET org_approval_status='rejected', is_approved=0 WHERE id IN ({','.join(['%s']*len(ids))}) AND org_id = %s", ids + [session.get('org_id')])
    conn.commit()
    cursor.close()
    conn.close()
    log_audit_event('org_admin_reject_voters', target_type='user', details={'rejected_ids': ids, 'count': len(ids), 'org_id': session.get('org_id')})
    return jsonify({"ok": True, "updated": len(ids)})



@app.route('/admin_settings.html', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_settings():
    org = None
    if session.get('org_id'):
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM orgs WHERE id = %s", (session.get('org_id'),))
        org = cursor.fetchone()
        cursor.close()
        conn.close()

    if request.method == 'POST':
        full_name = request.form.get('full_name', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '').strip()

        org_name = request.form.get('org_name', '').strip()
        date_of_birth = request.form.get('date_of_birth', '').strip()
        gov_id = request.form.get('gov_id', '').strip()
        department = request.form.get('department', '').strip()
        org_code = request.form.get('org_code', '').strip()

        if not full_name or not email:
            flash('Name and email are required.', 'danger')
            return redirect(url_for('admin_settings'))

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id FROM users WHERE email = %s AND id != %s", (email, session.get('user_id')))
        exists = cursor.fetchone()
        if exists:
            cursor.close()
            conn.close()
            flash('Email already in use.', 'danger')
            return redirect(url_for('admin_settings'))

        if password:
            pw_hash = bcrypt.generate_password_hash(password).decode('utf-8')
            cursor.execute("UPDATE users SET full_name=%s, email=%s, password_hash=%s WHERE id=%s",
                           (full_name, email, pw_hash, session.get('user_id')))
        else:
            cursor.execute("UPDATE users SET full_name=%s, email=%s WHERE id=%s",
                           (full_name, email, session.get('user_id')))
        conn.commit()

        # Update org profile (only if org exists)
        if session.get('org_id'):
            # handle logo upload
            logo_file = request.files.get('org_logo')
            logo_path = None
            if logo_file and logo_file.filename:
                safe_name = secure_filename(logo_file.filename)
                ext = os.path.splitext(safe_name)[1] or '.png'
                filename = f"org_{session.get('org_id')}_{int(time.time())}{ext}"
                save_path = os.path.join(UPLOAD_FOLDER, filename)
                logo_file.save(save_path)
                logo_path = f"uploads/{filename}"

            if org_code:
                # ensure unique org code
                cursor.execute("SELECT id FROM orgs WHERE org_code = %s AND id != %s", (org_code, session.get('org_id')))
                org_exists = cursor.fetchone()
                if org_exists:
                    cursor.close()
                    conn.close()
                    flash('Organization ID already in use.', 'danger')
                    return redirect(url_for('admin_settings'))

            if logo_path:
                cursor.execute("UPDATE orgs SET org_name=%s, org_code=%s, org_logo_path=%s WHERE id=%s",
                               (org_name or (org.get('org_name') if org else ''), org_code or (org.get('org_code') if org else ''), logo_path, session.get('org_id')))
            else:
                cursor.execute("UPDATE orgs SET org_name=%s, org_code=%s WHERE id=%s",
                               (org_name or (org.get('org_name') if org else ''), org_code or (org.get('org_code') if org else ''), session.get('org_id')))
            conn.commit()

        cursor.close()
        conn.close()

        session['name'] = full_name
        session['email'] = email
        flash('Profile updated.', 'success')
        return redirect(url_for('admin_settings'))

    return render_template('admin_settings.html', org=org)


if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5000)),
        debug=os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    )
