from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
import pandas as pd
import re, sys

def is_training_row(row):
    return len(row) == 10 and \
        list(list(list(row.children)[5].children)[0].children)[0] == 'T'

def parse_minutes(text):
    sep = text.index("+")
    hours = int(text[0:sep])
    minutes = int(text[sep+1:])
    return (minutes + hours*60)

def parse_cruart_date(text):
    p = re.compile("(.*)/(.*)/(.*)")
    m = p.match(text)
    month = int(m.group(1))
    day = int(m.group(2))
    year = int(m.group(3))
    return (year,month,day)
    

def main(filename):
    columns=["trainee", "minutes", "training_date", "training_pos", "ojti", "t_date"]
    results = pd.DataFrame(columns = columns)
    
    tz = ZoneInfo("America/Detroit")
    with open(filename) as f:
        html_txt = f.read()
    html_data = BeautifulSoup(html_txt, "html.parser")

    tables = html_data.find_all("table")

    for table in tables:
        for row in table.find_all("tr"):
            if is_training_row(row):
                cols = row.find_all("td")
                trainee = cols[3].font.string[-2:]
                minutes = parse_minutes(cols[9].font.string)
                date = parse_cruart_date(cols[6].font.string)
                time = cols[7].font.string
                dt = datetime(*date, int(time[0:2]), int(time[3:5]), tzinfo=timezone.utc)
                local_dt = dt.astimezone(tz).strftime("%m/%d/%Y %H:%M")
                pos = cols[4].font.string
                ojti = cols[2].font.string[-2:]
                if pos == "RCIC" or pos == "CICA":
                    continue
                t_date = dt.strftime("%Y%m%d")
                df = pd.DataFrame([[trainee,minutes,local_dt,pos,ojti,t_date]], columns=columns)
                results = pd.concat([results,df],ignore_index=True)
    outfile = re.compile("(.*)\.htm").match(filename).group(1) + ".csv"
    results.to_csv(outfile, index=False)

main(sys.argv[1])
