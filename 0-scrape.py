import requests
from lib.communes import communes
import os
import json 
import uuid

# https://informesdeis.minsal.cl/SASVisualAnalytics/?reportUri=/reports/reports/ad0c03ad-ee7a-4da4-bcc7-73d6e12920cf&sso_guest=true&reportViewOnly=true&reportContextBar=false&sas-welcome=false
xCsrfToken = ""
jSessionID = ""

# load the payloads
class Report:
    def __init__(self, name: str, payload: str):
        self.name = name
        self.payload = payload

        # rescue the metadata stored inside the payload
        temp = json.loads(payload)
        self.family = temp["_type"]
        self.report = temp["_report"]
        try:
            self.misc = temp["_extra"]
        except:
            self.misc = None

print("loading reports")
reports: list[Report] = []
files = os.listdir("payloads/")
for file in files:
    path = "payloads/" + file
    with open(path, "r") as f:
        payload = f.read()
        reports.append(Report(file, payload))

# get an executor id
print("getting executor id")
url = "https://informesdeis.minsal.cl/reportData/executors"
response = requests.post(
    url, 
    headers = {
        "Content-Type": "application/vnd.sas.report.query+json",
        "x-csrf-token": xCsrfToken,
        "Cookie": "JSESSIONID=" + jSessionID
    },
)
try:
    executorID: str = response.json()["id"]
except:
    print(response.text)
    raise Exception("failed to get executor id")

# main scrape
print("scraping")
sequence = 0
unscraped = 0

existing_responses = os.listdir("responses/")

for commune in communes:
    for establishment in commune.establishments:
        for report in reports:

            # verify if the report already exists
            filename = str(commune.name) + "-" + str(establishment) + "-" + report.name
            path = "responses/" + filename

            # if we already scrapped this check if it failed
            if(filename in existing_responses):
                f = open(path)
                data = json.load(f)

                if data["results"][0]["status"] == "failure":
                    # go ahead and download again
                    pass
                else:
                    # if it downloaded correctly skip it
                    continue

            sequence += 1

            # replace the {1} in the payload with the establishment
            payload = report.payload.replace("{1}", establishment)

            # format the payload
            payload = json.loads(payload)

            # build url
            url = f"https://informesdeis.minsal.cl/reportData/jobs?indexStrings=true&embeddedData=true&wait=30" 
            url += "&executorId=" + executorID
            url += "&jobId=" + str(uuid.uuid4())
            url += "&sequence=" + str(sequence)

            # ask for the report
            # retry once if it fails
            try:
                response = requests.post(
                    url,
                    headers={
                        "content-type": "application/vnd.sas.report.query+json",
                        "x-csrf-token": xCsrfToken,
                    },
                    cookies={
                        "JSESSIONID": jSessionID
                    },
                    json = payload,
                )
            except requests.exceptions.RequestException as e:
                print("request failed, retrying...")
                response = requests.post(
                    url,
                    headers={
                        "content-type": "application/vnd.sas.report.query+json",
                        "x-csrf-token": xCsrfToken,
                    },
                    cookies={
                        "JSESSIONID": jSessionID
                    },
                    json = payload,
                )

            content = json.loads(response.text)["results"]["content"]
            content = json.loads(content)

            if(content["results"][0]["status"] == "failure"):
                raise Exception("failed to download " + path)

            content["establishment"] = establishment
            content["commune"] = commune.name
            content["report"] = report.report
            content["family"] = report.family
            content["misc"] = report.misc

            content = json.dumps(content, ensure_ascii=False, indent=4)

            # save the report
            with open(path, "w") as f:
                f.write(content)
            
            print(f"{path} saved successfully")


print("scrapping done")
print("unscraped reports: " + str(unscraped))
print("scrapped reports: " + str(sequence - unscraped))