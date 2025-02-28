API_URL = "https://www.myskillsfuture.gov.sg/services/tex/individual/course-search"
API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.myskillsfuture.gov.sg/content/portal/en/portal-search/portal-search.html",
    "X-Requested-With": "XMLHttpRequest",
}
QUERY_ROWS = 24
PROJECT_ID = "jeremy-chia"
BIGQUERY_TABLES = {
    "courses": "sg_skillsfuture.courses",
    "training_areas": "sg_skillsfuture.training_areas",
    "languages": "sg_skillsfuture.languages",
    "featured_initiatives": "sg_skillsfuture.featured_initiatives",
    "skillsfuture_initiatives": "sg_skillsfuture.skillsfuture_initiatives",
}
PRIMARY_KEYS = {
    "courses": ["course_reference_number"],
    "training_areas": ["course_reference_number", "area_of_training_id"],
    "languages": ["course_reference_number", "language_of_instruction"],
    "featured_initiatives": ["course_reference_number", "featured_initiatives_tag"],
    "skillsfuture_initiatives": [
        "course_reference_number",
        "skillsfuture_initiatives_tag",
    ],
}
