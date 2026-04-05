@echo off
:: CCDG Weekly Scoring Run
:: Scheduled via Windows Task Scheduler — runs Monday mornings.
:: Uses the project virtual environment directly (no activation needed).
:: Output is captured by the script's own logger (logs/YYYY-MM-DD.log).

cd /d "d:\Code\CCDG\CCDG-Scorkeeping2026"
"d:\Code\CCDG\CCDG-Scorkeeping2026\.venv\Scripts\python.exe" ccdg__main_2026.py
