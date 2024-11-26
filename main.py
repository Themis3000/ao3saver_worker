import time
from multiprocessing import Process
from typing import List
import requests
import os
from bs4 import BeautifulSoup
import sys, signal

task_interval = int(os.environ.get("TASK_INTERVAL", 5))
server_address = os.environ["DL_SCRIPT_ADDRESS"]
download_timeout = int(os.environ.get("DOWNLOAD_TIMEOUT", 240))

admin_token_str = os.environ.get("ADMIN_TOKEN", None)
auth_header = {"token": admin_token_str}
client_name = os.environ["DL_SCRIPT_NAME"]
request_endpoint = f"{server_address}/request_job"
failed_endpoint = f"{server_address}/job_fail"
submit_endpoint = f"{server_address}/submit_job"


def signal_handler(signal, frame):
    print("\nexiting worker...")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

proxies = {}
if "PROXYADDRESS" in os.environ:
    proxies = {"https": os.environ["PROXYADDRESS"]}

formats = {"pdf": "application/pdf",
           "epub": "application/epub+zip",
           "html": "text/html",
           "azw3": "application/vnd.amazon.ebook",
           "mobi": "application/x-mobipocket-ebook"}


def do_task():
    job_res = requests.post(request_endpoint, headers=auth_header, json={"client_name": client_name})
    job_info = job_res.json()
    print(job_info)

    if job_info["status"] == "queue empty":
        print("No jobs available in queue")
        return

    print(f"downloading {job_info['work_id']} updated at {job_info['updated']} in {job_info['work_format']} format...")
    dl_response = requests.get(
        f"https://download.archiveofourown.org/downloads/{job_info['work_id']}/file.{job_info['work_format']}"
        f"?updated_at={job_info['updated']}",
        proxies=proxies)
    if not dl_response.ok or dl_response.headers["Content-Type"] != formats[job_info["work_format"]]:
        print(f"got response {dl_response.status_code} when requesting {job_info['work_id']} updated at"
              f" {job_info['updated']} in {job_info['work_format']} format, reporting to server...")
        fail_response = requests.post(failed_endpoint,
                                      headers=auth_header,
                                      json={
                                          "dispatch_id": job_info["dispatch_id"],
                                          "report_code": job_info["report_code"],
                                          "fail_status": dl_response.status_code})
        if not fail_response.ok:
            print("couldn't report failed job")
            return
        print("Fail report success")
        return
    data = dl_response.content

    print(f"successfully downloaded {job_info['work_id']} updated at {job_info['updated']}, reporting to server...")
    submit_res = requests.post(submit_endpoint,
                               headers=auth_header,
                               files=[("work", ("work", data, ""))],
                               data={
                                   "dispatch_id": job_info["dispatch_id"],
                                   "report_code": job_info["report_code"]})

    if not submit_res.ok:
        print(f"Work report has failed")
        return

    print("Work report success!")

    submit_json = submit_res.json()
    if len(submit_json["unfetched_objects"]) <= 0:
        print("No unfetched objects found. Ending job.")
        return

    for unfetched_object in submit_json["unfetched_objects"]:
        print(f"fetching {unfetched_object}")
        headers = {"User-Agent": "Mozilla/5.0"}
        if unfetched_object.get("etag"):
            print(f"found cache info for {unfetched_object['request_url']}")
            headers["If-None-Match"] = unfetched_object['potential_etag']

        try:
            obj_response = requests.get(unfetched_object['request_url'], headers=headers, proxies=proxies)
        except Exception:
            print(f"failed to download unfetched object {unfetched_object}")
            continue

        if not obj_response.ok:
            print(f"failed to download unfetched object {unfetched_object} with response {obj_response.status_code}")
            continue

        if obj_response.status_code == 304:
            print(f"object cache hit for {unfetched_object}")
            continue


if __name__ == "__main__":
    if os.environ.get("DEBUG_MODE", "FALSE").lower() == "true":
        do_task()
        exit()

    processes = []
    while True:
        for i, process_entry in enumerate(processes):
            process, process_start_time = process_entry
            if not process.is_alive():
                processes.pop(i)
                continue
            if time.time() - process_start_time > download_timeout:
                process.kill()
                processes.pop(i)

        if 2 > len(processes):
            process = Process(target=do_task)
            process.start()
            processes.append((process, time.time()))

        time.sleep(task_interval)
