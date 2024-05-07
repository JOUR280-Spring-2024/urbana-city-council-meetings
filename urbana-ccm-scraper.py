import requests
from bs4 import BeautifulSoup
import pdfplumber
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, String
from datetime import datetime
from sqlalchemy import select
from sqlalchemy import and_

engine = create_engine('sqlite:///urbana_council_meetings.sqlite')

metadata = MetaData()
meetings = Table('meetings', metadata,
                 Column('date', String, primary_key=True),
                 Column('title', String, primary_key=True),
                 Column('link_agenda', String),
                 Column('text_agenda', String),
                 Column('link_agenda_packet', String),
                 Column('text_agenda_packet', String),
                 Column('link_minutes', String),
                 Column('text_minutes', String),
                 Column('link_video', String))
metadata.create_all(engine)

session = requests.Session()
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 '
                  'Safari/537.36'}
stop_loop = False
page = 0
with engine.connect() as connection:
    while not stop_loop:
        url = f"https://urbana-il.municodemeetings.com/?page={page}"
        page = page + 1
        response = session.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        tbody = soup.select_one('tbody')
        if tbody is None:
            print("Finished.", page)
            stop_loop = True
        else:
            print(page)
            for table_row in tbody.select('tr'):
                date = table_row.select_one('span.date-display-single')
                date_db = date.text.strip()
                date_db = datetime.strptime(date_db, "%m/%d/%Y - %I:%M%p").strftime("%Y-%m-%d %H:%M")
                title = table_row.select_one('td.views-field-title')
                title_db = title.text.strip()
                agendas = table_row.select_one('td.views-field-field-agendas a')
                if agendas is not None:
                    response = session.get(agendas['href'], headers=headers)
                    with open('agenda.pdf', 'wb') as f:
                        f.write(response.content)
                    pdf = pdfplumber.open("agenda.pdf")
                    text_agenda = ""
                    link_agenda = agendas['href']
                    print(link_agenda)
                    for pdf_page in pdf.pages:
                        text_agenda = text_agenda + pdf_page.extract_text()
                else:
                    text_agenda = None
                    link_agenda = None
                packets = table_row.select_one('td.views-field-field-packets a')
                if packets is not None:
                    response = session.get(packets['href'], headers=headers)
                    with open('packets.pdf', 'wb') as f:
                        f.write(response.content)
                    pdf = pdfplumber.open("packets.pdf")
                    text_packets = ""
                    link_packets = packets['href']
                    print(link_packets)
                    for pdf_page in pdf.pages:
                        text_packets = text_packets + pdf_page.extract_text()
                else:
                    text_packets = None
                    link_packets = None
                minutes = table_row.select_one('td.views-field-field-minutes a')
                if minutes is not None:
                    response = session.get(minutes['href'], headers=headers)
                    with open('minutes.pdf', 'wb') as f:
                        f.write(response.content)
                    pdf = pdfplumber.open("minutes.pdf")
                    text_minutes = ""
                    link_minutes = minutes['href']
                    print(link_minutes)
                    for pdf_page in pdf.pages:
                        text_minutes = text_minutes + pdf_page.extract_text()
                else:
                    text_minutes = None
                    link_minutes = None
                videos = table_row.select_one('td.views-field-field-video-link a')
                if videos is not None:
                    video_db = videos['href']
                else:
                    video_db = None
                insert_query = insert(meetings).values(
                    date=date_db,
                    title=title_db,
                    link_agenda=link_agenda,
                    text_agenda=text_agenda,
                    link_agenda_packet=link_packets,
                    text_agenda_packet=text_packets,
                    link_minutes=link_minutes,
                    text_minutes=text_minutes,
                    link_video=video_db
                )
                connection.execute(insert_query.on_conflict_do_update(
                    index_elements=['date', 'title'],
                    set_=dict(link_agenda=link_agenda,
                              text_agenda=text_agenda,
                              link_agenda_packet=link_packets,
                              text_agenda_packet=text_packets,
                              link_minutes=link_minutes,
                              text_minutes=text_minutes,
                              link_video=video_db
                              )
                ))
                connection.commit()

    links = []
    stmt = select(meetings.c.date,
                  meetings.c.title,
                  meetings.c.link_agenda).where(and_(meetings.c.link_agenda is not None,
                                                     meetings.c.text_agenda is None))
    for row in connection.execute(stmt):
        links.append({'date': row.date, 'title': row.title, 'link': row.link_agenda_packet, 'type': "agenda"})
    stmt = select(meetings.c.date,
                  meetings.c.title,
                  meetings.c.link_agenda_packet).where(and_(meetings.c.link_agenda_packet is not None,
                                                            meetings.c.text_agenda_packet is None))
    for row in connection.execute(stmt):
        links.append({'date': row.date, 'title': row.title, 'link': row.link_agenda_packet, 'type': "agenda_packet"})
    stmt = select(meetings.c.date,
                  meetings.c.title,
                  meetings.c.link_minutes).where(and_(meetings.c.link_minutes is not None,
                                                      meetings.c.text_minutes is None))
    for row in connection.execute(stmt):
        links.append({'date': row.date, 'title': row.title, 'link': row.link_agenda_packet, 'type': "minutes"})

    num_links = 1
    for link in links:
        print(f"{num_links}/{len(links)} - {link['link']}")
        response = session.get(link['link'], headers=headers)
        with open('file.pdf', 'wb') as f:
            f.write(response.content)
        with pdfplumber.open("file.pdf") as pdf:
            text = ""
            for pdf_page in pdf.pages:
                text = text + pdf_page.extract_text()


