"""Clear only fr_task_pool rows, after the workflow has stopped older runs."""
import json
import os
import sys
import urllib.parse
import urllib.request
import urllib.error
import time

ENDPOINT = "https://unapiwajmgqqtdkixaab.supabase.co/functions/v1/fr-task-pool-clear"
AUDIENCE = "foodradar:task-pool-clear"

def request_json(url, headers, body=None):
    request = urllib.request.Request(url, data=body, headers=headers)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code < 500 or attempt == 2:
                raise RuntimeError(f"Task pool reset HTTP {exc.code}") from None
        except (OSError, TimeoutError):
            if attempt == 2:
                raise RuntimeError("Task pool reset connection failed") from None
        time.sleep(2 ** attempt)

def main():
    oidc_url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
    separator = "&" if "?" in oidc_url else "?"
    token_url = oidc_url + separator + urllib.parse.urlencode({"audience": AUDIENCE})
    token = request_json(token_url, {
        "Authorization": "Bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
    })["value"]
    result = request_json(ENDPOINT, {
        "X-FR-Workflow-Token": token,
        "Content-Type": "application/json",
    }, b"{}")
    if (str(result.get("run_id")) != os.environ["GITHUB_RUN_ID"]
            or int(result.get("run_attempt", 0)) != int(os.environ["GITHUB_RUN_ATTEMPT"])
            or type(result.get("deleted")) is not int or result["deleted"] < 0):
        raise RuntimeError("Unexpected SB task-pool clear confirmation")
    message = f"已清除 {result['deleted']} 筆 SB 暫存任務；保留資料表及欄位設定"
    print(message)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write(message + "\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("任務池清空失敗，禁止爬取：" + str(exc), file=sys.stderr)
        sys.exit(1)
