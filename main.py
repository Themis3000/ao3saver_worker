import time
from typing import List
import requests
import os
from bs4 import BeautifulSoup

task_interval = int(os.environ.get("TASK_INTERVAL", 5))
server_address = os.environ["DL_SCRIPT_ADDRESS"]

admin_token_str = os.environ.get("ADMIN_TOKEN", None)
auth_header = {"token": admin_token_str}
client_name = os.environ["DL_SCRIPT_NAME"]
request_endpoint = f"{server_address}/request_job"
failed_endpoint = f"{server_address}/job_fail"
submit_endpoint = f"{server_address}/submit_job"

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

    # fetch any images, if this is a html page.
    images = []
    images_meta = {}
    if job_info["work_format"] == "html":
        soup = BeautifulSoup(data, 'html.parser')
        img_urls: List[str] = [img['src'] for img in soup.find_all('img', src=True)]
        for i, url in enumerate(img_urls):
            cache_info = job_info["cache_infos"].get(url, None)
            headers = {}
            if cache_info:
                print(f"found cache info for {url}")
                headers["If-None-Match"] = cache_info["etag"]

            print(f"fetching image {url}")
            img_response = requests.get(url, headers=headers, proxies=proxies)

            if img_response.status_code == 304:
                print("Image hit cache!")
                images_meta[f"cached_{i}_object_id"] = cache_info["object_id"]
                images_meta[f"cached_{i}_url"] = cache_info["url"]
                continue

            if not img_response.ok:
                print("couldn't fetch image")
                continue

            images.append((f"supporting_objects_{i}", (
                url.split("/")[-1],
                img_response.content,
                img_response.headers.get("Content-Type", "")
            )))
            images_meta[f"supporting_objects_{i}_url"] = url
            images_meta[f"supporting_objects_{i}_etag"] = img_response.headers.get("ETag", "")
            print("fetched image!")

    print(f"successfully downloaded {job_info['work_id']} updated at {job_info['updated']}, reporting to server...")
    submit_res = requests.post(submit_endpoint,
                               headers=auth_header,
                               files=[("work", ("work", data, "")), *images],
                               data={
                                   "dispatch_id": job_info["dispatch_id"],
                                   "report_code": job_info["report_code"], **images_meta})

    if not submit_res.ok:
        print(f"Work report has failed")
        return

    print("Work report success!")


last_task_time = time.time()
while True:
    time_since_last_task = time.time() - last_task_time
    if time_since_last_task >= task_interval:
        do_task()
        last_task_time = time.time()
    else:
        time.sleep(task_interval - time_since_last_task)
