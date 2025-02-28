import requests
from course_details.config import BASE_URL, COURSE_DETAIL_URL_TEMPLATE


def fetch_course_details(course_reference_number):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": COURSE_DETAIL_URL_TEMPLATE.format(course_reference_number),
    }
    params = {
        "action": "get-course-by-ref-number",
        "refNumber": course_reference_number,
    }
    response = requests.get(BASE_URL, headers=headers, params=params)

    if response.status_code == 200:
        print(f"Data for course {course_reference_number} downloaded")
        return response
    else:
        print(
            f"Failed to retrieve data for course {course_reference_number}: {response.status_code}"
        )
        return None
