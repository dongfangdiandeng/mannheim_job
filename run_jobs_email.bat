@echo off
cd /d %~dp0
python .\mannheim_jobs_bot.py --send-email --force-email --db .\jobs.db
pause
