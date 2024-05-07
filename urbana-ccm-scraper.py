import requests
from bs4 import BeautifulSoup
import pdfplumber
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, String
from datetime import datetime
from sqlalchemy import select
from sqlalchemy import and_
from sqlalchemy import update

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
            print(f"Page: {page}", url)
            for table_row in tbody.select('tr'):
                date = table_row.select_one('span.date-display-single')
                date_db = date.text.strip()
                date_db = datetime.strptime(date_db, "%m/%d/%Y - %I:%M%p").strftime("%Y-%m-%d %H:%M")
                title = table_row.select_one('td.views-field-title')
                title_db = title.text.strip()
                agendas = table_row.select_one('td.views-field-field-agendas a')
                if agendas is not None:
                    link_agenda = agendas['href']
                else:
                    link_agenda = None
                text_agenda = None
                packets = table_row.select_one('td.views-field-field-packets a')
                if packets is not None:
                    link_packets = packets['href']
                else:
                    link_packets = None
                text_packets = None
                minutes = table_row.select_one('td.views-field-field-minutes a')
                if minutes is not None:
                    link_minutes = minutes['href']
                else:
                    link_minutes = None
                text_minutes = None
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
                    set_=dict(
                        link_agenda=insert_query.excluded.link_agenda,
                        link_agenda_packet=insert_query.excluded.link_agenda_packet,
                        link_minutes=insert_query.excluded.link_minutes,
                        link_video=insert_query.excluded.link_video
                    )
                ))
    connection.commit()

    links = []
    stmt = select(meetings.c.date,
                  meetings.c.title,
                  meetings.c.link_agenda).where(and_(meetings.c.link_agenda != None,
                                                     meetings.c.text_agenda == None))
    for row in connection.execute(stmt):
        links.append({'date': row.date, 'title': row.title, 'link': row.link_agenda, 'type': "agenda"})
    stmt = select(meetings.c.date,
                  meetings.c.title,
                  meetings.c.link_agenda_packet).where(and_(meetings.c.link_agenda_packet != None,
                                                            meetings.c.text_agenda_packet == None))
    for row in connection.execute(stmt):
        links.append({'date': row.date, 'title': row.title, 'link': row.link_agenda_packet, 'type': "agenda_packet"})
    stmt = select(meetings.c.date,
                  meetings.c.title,
                  meetings.c.link_minutes).where(and_(meetings.c.link_minutes != None,
                                                      meetings.c.text_minutes == None))
    for row in connection.execute(stmt):
        links.append({'date': row.date, 'title': row.title, 'link': row.link_minutes, 'type': "minutes"})

    num_links = 1
    for link in links:
        print(f"{num_links}/{len(links)} - {link['link']}")
        num_links = num_links + 1
        response = session.get(link['link'], headers=headers)
        with open('file.pdf', 'wb') as f:
            f.write(response.content)
        try:
            with pdfplumber.open("file.pdf") as pdf:
                text = ""
                for pdf_page in pdf.pages:
                    text = text + pdf_page.extract_text()
            if link['type'] == 'agenda':
                stmt = update(meetings).where(and_(meetings.c.date == link['date'],
                                                   meetings.c.title == link['title'])).values(text_agenda=text)
            if link['type'] == 'agenda_packet':
                stmt = update(meetings).where(and_(meetings.c.date == link['date'],
                                                   meetings.c.title == link['title'])).values(text_agenda_packet=text)
            if link['type'] == 'minutes':
                stmt = update(meetings).where(and_(meetings.c.date == link['date'],
                                                   meetings.c.title == link['title'])).values(text_minutes=text)
            connection.execute(stmt)
            connection.commit()
        except Exception as e:
            print("Error reading PDF!")
