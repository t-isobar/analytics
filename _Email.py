#!usr/bin/python3

import sys
import imaplib, email
from email.header import decode_header
from datetime import datetime, timedelta
import _BigQuery

bq = _BigQuery.BigQuery("/home/isobar/connector/bq-json-file/t-isobar-bq.json")

def get_email(email_user, email_password, email_from, db_name, table_name, subject_filter, path_to_save, host='imap.yandex.ru', port=993):
    mail = imaplib.IMAP4_SSL(host, port)
    mail.login(email_user, email_password)
    mail.select('INBOX')

    since = datetime.strftime(datetime.today(), "%d-%b-%Y")
    before = datetime.strftime(datetime.today() + timedelta(days=1), "%d-%b-%Y")

    status_search, search_result = mail.uid('search', None, f'(FROM "{email_from}")', f'(SINCE "{since}")', f'(BEFORE "{before}")')
    list_of_ids_str = search_result[0].decode("utf-8").split(" ")
    if list_of_ids_str[0] == '':
        sys.exit(f"От {email_from} нет писем за {since}.")
    else:
        list_of_ids_int = [int(num) for num in list_of_ids_str]
        list_of_ids_in_one = ','.join(list_of_ids_str)
        ids_in_bq = bq.get_query(f"SELECT * FROM `{db_name}.{table_name}` WHERE letter_id IN ({list_of_ids_in_one})")
        list_for_download = list(set(list_of_ids_int).difference(set([ids[0] for ids in ids_in_bq])))

        if list_for_download == []:
            sys.exit(f"От {email_from} нет писем для скачивания.")

        for letter_id in list_for_download:
            mail.select('INBOX')
            status_result, letter_data = mail.uid('fetch', str(letter_id), '(RFC822)')
            raw_email = letter_data[0][1]
            email_message = email.message_from_bytes(raw_email)
            subject_list = decode_header(email_message['Subject'])
            code = subject_list[0][1]
            subject = subject_list[0][0].decode(code)


            if subject_filter in subject:
                list_of_file_names = []
                for part in email_message.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    if part.get('Content-Disposition') is None:
                        continue
                    file_name_list = decode_header(part.get_filename())
                    file_name = file_name_list[0][0]
                    code = file_name_list[0][1]
                    if code != None:
                        file_name = file_name_list[0][0].decode(code)
                    fp = open(f"{path_to_save}{file_name}", 'wb')
                    fp.write(part.get_payload(decode=True))
                    fp.close()
                    list_of_file_names.append(file_name)
                mail.close()
        return list_of_file_names