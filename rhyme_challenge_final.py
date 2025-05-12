"""
Rhyme Data Engineering Challenge
Author: Sowjanya Addanki

Description:
- Normalize and join user and event data using email
- Clean names and detect duplicates
- Generate final joined output, unmatched events, and event summary
"""

import pandas as pd
import re
import unicodedata
import logging

# ----------------------------
# Step 1: Setup logging
# ----------------------------
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# ----------------------------
# Step 2: Load input files
# ----------------------------
users = pd.read_csv('users.csv')
events = pd.read_csv('events.csv')

# ----------------------------
# Step 3: Normalize and clean email addresses
# ----------------------------
def normalize_email(email):
    if pd.isna(email):
        return email
    email = str(email).strip().lower()
    email = email.replace('\u200B', '').replace('\u00A0', '')  # Remove invisible characters
    email = unicodedata.normalize('NFKC', email)
    return email

users['email_clean'] = users['email'].apply(normalize_email)
events['user_email_clean'] = events['user_email'].apply(normalize_email)

# ----------------------------
# Step 4: Split full name into first and last
# ----------------------------
def split_name(full_name):
    cleaned = re.sub(r'\b(Dr\.?|Mr\.?|Ms\.?|Mrs\.?|Jr\.?|Sr\.?)\b', '', full_name)
    cleaned = cleaned.replace('.', ' ').strip()
    tokens = [t.strip(",.") for t in cleaned.split()]
    if len(tokens) == 0:
        return "", ""
    elif len(tokens) == 1:
        return tokens[0], ""
    else:
        return tokens[0], tokens[-1]

def split_name_to_series(name):
    first, last = split_name(name)
    return pd.Series([first, last])

users[['first_name', 'last_name']] = users['name'].apply(split_name_to_series)

# ----------------------------
# Step 5: Detect duplicate emails
# ----------------------------
duplicate_emails = users[users.duplicated('email_clean', keep=False)]
duplicate_emails.to_csv('duplicate_emails.csv', index=False)
logging.info("📄 duplicate_emails.csv saved.")

# ----------------------------
# Step 6: Merge users and events
# ----------------------------
merged = events.merge(users, left_on='user_email_clean', right_on='email_clean', how='left')

# ----------------------------
# Step 7: Create final output
# ----------------------------
final_output = merged[['user_id', 'first_name', 'last_name', 'event_type', 'timestamp', 'signup_date']]
final_output = final_output.dropna(subset=['user_id'])
final_output.to_csv('joined_events.csv', index=False)
logging.info("✅ joined_events.csv saved.")

# ----------------------------
# Step 8: Save unmatched event IDs
# ----------------------------
unmatched = merged[merged['user_id'].isna()]
unmatched[['event_id']].to_csv('unmatched_event_ids.csv', index=False)
logging.info("⚠️ unmatched_event_ids.csv saved.")

# ----------------------------
# Step 9: Generate summary table
# ----------------------------
event_counts = final_output.groupby('user_id').size().reset_index(name='event_count')
event_counts.to_csv('user_event_counts.csv', index=False)
logging.info("📊 user_event_counts.csv saved.")