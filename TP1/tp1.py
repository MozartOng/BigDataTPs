import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

rows = []
failed_pages = []

for page in range(1, 51):
    url = f"https://books.toscrape.com/catalogue/page-{page}.html"

    response = None
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            print(f"Page {page}, attempt {attempt + 1} failed: {type(e).__name__}")
            response = None
            time.sleep(3)

    if response is None:                          
        failed_pages.append(page)
        continue                                

    response.encoding = "utf-8"
    soup = BeautifulSoup(response.text, "html.parser")

    books = soup.find_all("article", class_="product_pod")
    for book in books:
        title = book.find("h3").find("a")["title"]
        price = book.find("p", class_="price_color").text
        rows.append({"title": title, "price": price})

    print(f"Page {page}: {len(books)} books")
    time.sleep(1)

df = pd.DataFrame(rows)
df.to_csv("books.csv", index=False, encoding="utf-8")
print("Rows:", len(df))
print("Failed pages:", failed_pages)