import re
from queue import Queue
from threading import Thread

import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE_URL = "https://www.talbotslaw.co.uk"
PROFILE_URLS = Queue()
PROFILES = Queue()

def get_profile_links() -> set[str]:
    response = requests.get("https://www.talbotslaw.co.uk/site/people/")
    soup = BeautifulSoup(response.text, "html.parser")

    staff_cards = soup.find_all(href=re.compile("^/site/people/profile/([^/]+)$"))
    urls_list= set()
    for staff_card in staff_cards:
        urls_list.add(staff_card["href"])

    for url in urls_list:
        PROFILE_URLS.put(url)

def scrapp_profiles_thread() -> None:
    
    while not PROFILE_URLS.empty():
        url = PROFILE_URLS.get()
        print(f"{BASE_URL}{url}")
        response = requests.get(f"{BASE_URL}{url}")
        soup = BeautifulSoup(response.text, 'html.parser')

        name = soup.find("li", "name").text
        position = soup.find("li", "job-title").text
        phone_number = soup.find("span", "profile-phone").text
        email = clean_email(soup.find("a", "profile-email")["href"])
        vcard_url = soup.find("a", "profile-vcard")["href"]

        job_desc_raw = soup.find("div", id="professional-biography").find_all("p")
        job_intro = job_desc_raw[0].text
        bio = clean_job_desc(job_desc_raw)

        profile = {
            "name": name,
            "email": email,
            "phone_number": phone_number,
            "position": position,
            "vcard_url": f"{BASE_URL}{vcard_url}",
            "job_intro": job_intro,
            "bio": bio,
            "link": f"{BASE_URL}{url}"
        }
        PROFILES.put(profile)

def clean_email(raw_email: str) -> str:
    return raw_email.replace("mailto:", "").split("?")[0]

def clean_job_desc(job_desc_raw) -> str:
    job_desc = ""
    job_desc_raw.pop(0)
    for desc in job_desc_raw:
        job_desc += desc.text
    return job_desc

def queue_to_list() -> list[dict[str,str]]:
    profile_list = []
    while not PROFILES.empty():
        profile = PROFILES.get()
        profile_list.append(profile)
    return profile_list

if __name__ == "__main__":
    
    get_profile_links()

    threads = [Thread(target=scrapp_profiles_thread) for _ in range(10)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    
    profiles = queue_to_list()

    df_profiles = pd.DataFrame(profiles)
    df_profiles.to_csv("profiles_scrapped_thread.csv", index=False)
    print(df_profiles)
