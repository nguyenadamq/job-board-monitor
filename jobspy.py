import csv
from jobspy import scrape_jobs

jobs = scrape_jobs(
    site_name=["indeed", "linkedin", "zip_recruiter", "google"], # "glassdoor", "bayt", "naukri", "bdjobs"
    search_term="software engineer",
    google_search_term="software engineer jobs near San Francisco, CA since yesterday",
    location="San Francisco, CA",
    results_wanted=20,
    hours_old=72,
    country_indeed='USA',
    
    linkedin_fetch_description=True, # gets more info such as description, direct job url (slower)
    proxies=["9JvKFKBq:3XCur035-cc-us@smrt.x.proxiess.com:7000"]
)
print(f"Found {len(jobs)} jobs")
print(jobs.head())
jobs.to_csv("jobs.csv", quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False) # to_excel