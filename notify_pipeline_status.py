# Databricks notebook source
# ---------------------------------------------------------------------------
# NOTIFICATION TASK (Notebook Task — NOT an SDP pipeline)
# ---------------------------------------------------------------------------
# Runs as the final task in the orchestration job with run_if: ALL_DONE
# so it fires regardless of whether bronze/silver/gold succeeded or failed.
#
# In a real deployment you would replace the print statements below with
# an actual notification: email, Slack webhook, PagerDuty, Teams message, etc.
# For now it logs a structured summary to the job run output.
# ---------------------------------------------------------------------------

print("=" * 60)
print("  MEDALLION PIPELINE — ORCHESTRATION NOTIFICATION")
print("=" * 60)
print()
print("run_if: ALL_DONE")
print()
print("This notification fires regardless of whether upstream tasks")
print("(bronze, silver, gold) succeeded or failed.")
print()
print("If you see this message after a bronze failure, it confirms")
print("acceptance criterion #3: silver/gold were skipped but the")
print("notification still fired.")
print()
print("=" * 60)
