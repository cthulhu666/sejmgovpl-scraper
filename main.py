import requests
import time
from bs4 import BeautifulSoup
from itertools import count
from google.cloud import bigquery


def scrape(url: str) -> tuple[bool, list[dict]]:
    rs = requests.get(url)
    if rs.status_code == 200:
        data = parse(rs.content)
        return True, data
    if rs.status_code == 404:
        return False, []
    else:
        raise RuntimeError(f"Unexpected http status: {rs.status_code}")


def parse(html_doc) -> list[dict]:
    soup = BeautifulSoup(html_doc, "html.parser")
    table = soup.find("table", {"class": "kluby"})
    data = []

    for row in table.find('tbody').find_all('tr'):
        cells = row.find_all('td')
        name = cells[0].text
        members = int(cells[1].text)
        votes = int(cells[2].text)
        yea = parse_vote(cells[3].text)
        nay = parse_vote(cells[4].text)
        abstain = parse_vote(cells[5].text)
        # print(f"{name}: {votes}/{members} {yea}-{nay}-{abstain}")
        data.append(dict(name=name, members=members, votes=votes, yea=yea, nay=nay, abstain=abstain))

    return data


def parse_vote(s: str) -> int:
    s = s.strip()
    if s == "-":
        return 0
    return int(s)


def loop(i, j):
    has_any = False
    for k in count(1):
        print(f"Scraping: kadencja {i} / posiedzenie {j} / głosowanie {k}")
        ok, data = scrape(
            f"https://sejm.gov.pl/Sejm9.nsf/agent.xsp?symbol=glosowania&nrkadencji={i}&nrposiedzenia={j}&nrglosowania={k}")
        if not ok:
            break
        for x in data:
            x['meeting'] = j
            x['voting'] = k
        save(data)
        has_any = True
    return has_any


def save(data: list[dict]):
    client = bigquery.Client(project="spry-sequence-341")

    dataset_id = "polish_parliament"
    table_id = f"{client.project}.{dataset_id}.votings"

    # dataset = bigquery.Dataset(dataset_id_full)
    # table_ref = dataset.table("votings")

    schema = [
        bigquery.SchemaField("meeting", "INTEGER"),
        bigquery.SchemaField("voting", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("members", "INTEGER"),
        bigquery.SchemaField("votes", "INTEGER"),
        bigquery.SchemaField("yea", "INTEGER"),
        bigquery.SchemaField("nay", "INTEGER"),
        bigquery.SchemaField("abstain", "INTEGER"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table_ref = client.create_table(table, exists_ok=True)

    errors = client.insert_rows(table_ref, data)
    print(errors)


if __name__ == "__main__":
    i = 9
    for j in count(1):
        if not loop(i, j):
            break
        time.sleep(1)
