from copy import deepcopy
from requests.adapters import HTTPAdapter
from unicodedata import normalize
from urllib3.util.retry import Retry
from datetime import datetime

from bs4 import BeautifulSoup
import requests
import toml


config = toml.load("config.toml")

NOTION_DATABASE_ID = config["Notion"]["DATABASE_ID"]
NOTION_SECRET = config["Notion"]["SECRET"]

LECTURES_SCHEDULE_URL = "https://yandex.ru/yaintern/schools/open-lectures"
API_DATABASE_QUERY = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
API_PAGE_CREATE = "https://api.notion.com/v1/pages"

HEADERS = {
    "Authorization": f"Bearer {NOTION_SECRET}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

LESSON_TRACKS = [
    "interfaces development",
    "backend (Python)",
    "backend (Java)",
    "backend (C++)",
    "backend (Go)",
    "mobile (Android)",
    "mobile (iOS)",
    "mobile (Flutter)",
    "management",
    "marketing",
    "product analytics"
]


def scrape_lessons(soup):
    lessons = []
    for el in soup.find_all("div", class_="lc-events-program__container"):
        data = el.find_all("div", class_="lc-styled-text__text")

        lesson_title = normalize("NFKD", data[1].text)
        lesson_desc = normalize("NFKD", data[2].text).strip().replace("\n", " ")
        lesson_date = data[0].text[:5].strip()
        lesson_link = el.find("a").attrs["href"] if el.find("a") else None

        lesson_speakers = []
        speakers_el = el.find_all("div", class_="lc-events-speaker__name")
        for speaker in speakers_el:
            lesson_speakers.append(speaker.text)

        lessons.append({
            "title": lesson_title,
            "link": lesson_link,
            "description": lesson_desc,
            "date": lesson_date,
            "speakers": lesson_speakers
        })
    return lessons


def process_lessons(lessons):
    """Process scraped lessons."""
    cur_track = -1
    for lesson in lessons:
        if lesson["date"] == "06.06":
            cur_track += 1
        lesson["track"] = [LESSON_TRACKS[cur_track]]
        lesson["date"] = datetime.strptime(lesson["date"], "%d.%m")\
            .replace(year=2023).isoformat().split("T")[0]
        lesson["link"] = lesson["link"].split("?")[0] if lesson["link"] else None
    return lessons


def merge_lessons(lessons):
    """Merge lessons with same video URLs."""
    urls = set((lesson["link"] for lesson in lessons))
    urls.remove(None)
    for url in urls:
        lesson_versions = [lesson for lesson in lessons if lesson['link'] == url]
        new_lesson = deepcopy(lesson_versions[0])
        new_lesson['track'] = [lesson['track'][0] for lesson in lesson_versions]
        for lesson in lesson_versions:
            lessons.remove(lesson)
        lessons.append(new_lesson)
    return lessons


def get_lessons_data(session):
    """Get lessons data from Yandex."""
    data = session.get(LECTURES_SCHEDULE_URL, headers={
        "Cookie": config["YANDEX_COOKIE"].encode(),
        "User-Agent": config["USER_AGENT"]
    })
    return BeautifulSoup(data.content, features="html.parser")


def create_lessons(session):
    lessons = merge_lessons(process_lessons(scrape_lessons(get_lessons_data(session))))

    processed = 0
    for lesson in lessons:
        response = create_page(lesson, session=session)
        if response.status_code == 200:
            print(f"Lesson {lesson['title']} was created successfully... {round(processed / len(lessons) * 100)}%")
        else:
            print(f"Error {response.status_code}\n{response.content}")
        processed += 1


def create_page(lesson_data, session):
    """Create a new lesson page in the database using Notion API."""
    page_data = {
        "Title": {"title": [{"text": {"content": lesson_data["title"]}}]},
        "Track": {"multi_select": [{"name": track} for track in lesson_data["track"]]},
        "Video": {"url": lesson_data["link"]},
        "Date": {"date": {"start": lesson_data["date"]}},
        "Lecturers": {"multi_select": [{"name": lecturer} for lecturer in lesson_data["speakers"]]},
        "Description": {"rich_text": [{"text": {"content": lesson_data["description"]}}]}
    }

    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": page_data
    }

    response = session.post(API_PAGE_CREATE, headers=HEADERS, json=payload)
    return response


def main():
    # Requests session initialization
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    create_lessons(session)


if __name__ == "__main__":
    main()
