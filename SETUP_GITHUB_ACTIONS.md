# GitHub Actions Quick Setup

1. Push this project to a GitHub repository.
2. In GitHub: `Settings` -> `Secrets and variables` -> `Actions` -> `New repository secret`.
3. Add these secrets:
   - `SMTP_HOST` (example: `smtp.qq.com`)
   - `SMTP_PORT` (example: `465`)
   - `SMTP_USER` (sender email)
   - `SMTP_PASS` (SMTP auth code, not login password)
   - `TO_EMAILS` (comma-separated recipients)
4. Open `Actions` tab and run `Mannheim Jobs Hourly` once with `Run workflow`.
5. Verify email arrives, then it will run every hour automatically.

Notes:
- Schedule cron is UTC (`0 * * * *` means every hour on minute 00 UTC).
- No personal email/password is stored in repo files.
- `jobs.db` is committed by workflow to keep incremental state.
