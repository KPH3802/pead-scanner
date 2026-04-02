# PEAD Scanner Configuration Example
# Copy to config.py and fill in real values

# --- FMP API ---
FMP_API_KEY = 'YOUR_FMP_API_KEY_HERE'
FMP_BASE_URL = 'https://financialmodelingprep.com'

# --- Database ---
DB_NAME = 'pead_signals.db'

# --- Signal thresholds ---
BEAT_THRESHOLD = 5.0     # % EPS beat -> BULL signal
MISS_THRESHOLD = 5.0     # % EPS miss -> BEAR signal
MIN_ABS_EPS    = 0.01    # minimum absolute estimated EPS (avoids near-zero noise)

# --- Q4 exclusion ---
Q4_EXCLUDE_MONTHS = [1, 2]

# --- Signal ---
ENABLE_BEAR = True

# --- Email ---
EMAIL_SENDER    = 'sender@gmail.com'
EMAIL_RECIPIENT = 'recipient@gmail.com'
EMAIL_PASSWORD  = 'app_password_here'
SMTP_SERVER     = 'smtp.gmail.com'
SMTP_PORT       = 587

