from connectors._Utils import slice_date_on_period
from connectors._BigQuery import BigQuery
from connectors._Calltouch import Calltouch
from connectors._Facebook import Facebook
from connectors._GAnalytics import GAnalytics
from connectors._GAnalyticsUpload import GAnalyticsUpload
from connectors._Hybrid import Hybrid
from connectors._MyTarget import MyTarget
from connectors._YandexDirectReports import YandexDirectReports
from connectors._YandexDirect import YandexDirect

import pandas as pd
import re
import sys
import imaplib, email
from email.header import decode_header
from datetime import datetime


class Report:
	def __init__(self, client_name, path_to_bq, path_to_ga):
		self.client_name = client_name
		self.path_to_ga = path_to_ga
		self.bq = BigQuery(path_to_bq)

	def get_calltouch_report(self, site_id, access_token, date_from, date_to):
		ct = Calltouch(site_id, access_token, self.client_name)
		data_set_id = f"{self.client_name}_Calltouch_{site_id}"

		self.bq.check_or_create_dataset(data_set_id)
		self.bq.check_or_create_tables(ct.tables_with_schema, data_set_id)

		calls = ct.get_calls(date_from, date_to)
		if calls == []:
			return f"За {date_from} - {date_to} нет статистики"

		calls_df = pd.DataFrame(calls).fillna("<not set>")
		self.bq.data_to_insert(calls_df, ct.integer_fields, ct.float_fields, ct.string_fields, data_set_id,
								f"{self.client_name}_Calltouch_CALLS")

		return f"За {date_from} - {date_to} статистика загружена"

	def get_hybrid_report(self, client_id, client_secret, path_to_json, date_from, date_to):
		hybrid = Hybrid(client_id, client_secret, self.client_name, path_to_json)

		data_set_id = f"{self.client_name}_Hybrid"

		self.bq.check_or_create_dataset(data_set_id)
		self.bq.check_or_create_tables(hybrid.tables_with_schema, data_set_id)

		campaigns = hybrid.get_campaigns()
		campaign_df = pd.DataFrame(campaigns)
		campaign_ids = campaign_df['Id'].tolist()
		self.bq.data_to_insert(campaign_df, hybrid.integer_fields, hybrid.float_fields, hybrid.string_fields, data_set_id,
								f"{self.client_name}_Hybrid_CAMPAIGNS")

		campaign_stat = hybrid.get_campaign_stat(campaign_ids, date_from, date_to)
		campaign_stat_df = pd.DataFrame(campaign_stat)
		self.bq.data_to_insert(campaign_stat_df, hybrid.integer_fields, hybrid.float_fields, hybrid.string_fields,
								data_set_id, f"{self.client_name}_Hybrid_CAMPAIGN_STAT")

		advertiser_stat = hybrid.get_advertiser_stat(date_from, date_to)
		advertiser_stat_df = pd.DataFrame(advertiser_stat)
		self.bq.data_to_insert(advertiser_stat_df, hybrid.integer_fields, hybrid.float_fields, hybrid.string_fields,
								data_set_id, f"{self.client_name}_Hybrid_ADVERTISER_STAT")

	def get_email_report(self, email_user, email_password, email_from, db_name, table_name, subject_filter,
						path_to_save, since, before, host='imap.yandex.ru', port=993):

		mail = imaplib.IMAP4_SSL(host, port)
		mail.login(email_user, email_password)
		mail.select('INBOX')

		# since = datetime.strftime(datetime.today(), "%d-%b-%Y")
		# before = datetime.strftime(datetime.today() + timedelta(days=1), "%d-%b-%Y")

		status_search, search_result = mail.uid('search', None, f'(FROM "{email_from}")', f'(SINCE "{since}")',
												f'(BEFORE "{before}")')
		list_of_ids_str = search_result[0].decode("utf-8").split(" ")
		if list_of_ids_str[0] == '':
			sys.exit(f"От {email_from} нет писем за {since}.")
		else:
			list_of_ids_int = [int(num) for num in list_of_ids_str]
			list_of_ids_in_one = ','.join(list_of_ids_str)
			ids_in_bq = self.bq.get_query(
				f"SELECT * FROM `{db_name}.{table_name}` WHERE letter_id IN ({list_of_ids_in_one})")
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

	def get_facebook_report(self, access_token, account, date_from, date_to):
		fb = Facebook(access_token, account, self.client_name)

		data_set_id = f"{self.client_name}_Facebook_{account[4:]}"

		self.bq.check_or_create_dataset(data_set_id)
		self.bq.check_or_create_tables(fb.tables_with_schema, data_set_id)

		ads_stat = fb.get_ads_or_campaign_statistics(date_from, date_to, "ad")

		if ads_stat == []:
			return f"За {date_from} - {date_to} нет статистики"

		ads_stat_df = pd.DataFrame(ads_stat).fillna(0)
		self.bq.data_to_insert(ads_stat_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
								f"{self.client_name}_Facebook_ADS_STAT")

		campaign_stat = fb.get_ads_or_campaign_statistics(date_from, date_to, "campaign")
		campaign_stat_df = pd.DataFrame(campaign_stat).fillna(0)
		self.bq.data_to_insert(campaign_stat_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
								f"{self.client_name}_Facebook_CAMPAIGN_STAT")

		campaigns_ids = campaign_stat_df['campaign_id'].tolist()

		campaigns, header = fb.get_campaigns([])
		campaigns_df = pd.DataFrame(campaigns)
		self.bq.insert_difference(campaigns_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
									f"{self.client_name}_Facebook_CAMPAIGNS", 'id', 'id')

		ads_basic = fb.get_ads_or_adsets(campaigns_ids, "ads-basic")
		ads_basic_df = pd.DataFrame(ads_basic).fillna("<not set>")
		self.bq.insert_difference(ads_basic_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
									f"{self.client_name}_Facebook_ADS_BASIC", 'id', 'id')

		ads_layout = fb.get_ads_or_adsets(campaigns_ids, "ads-layout")
		ads_layout_df = pd.DataFrame(ads_layout).fillna("<not set>")
		self.bq.insert_difference(ads_layout_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
									f"{self.client_name}_Facebook_ADS_LAYOUT", 'id', 'ad_id')

		ad_sets = fb.get_ads_or_adsets(campaigns_ids, "adsets")
		ad_sets_df = pd.DataFrame(ad_sets).fillna("<not set>")
		self.bq.insert_difference(ad_sets_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
									f"{self.client_name}_Facebook_ADSETS", 'id', 'id')

	def get_analytics_report(self, view_id, date_from, date_to, report_type):
		ga = GAnalytics(self.path_to_ga, view_id, self.client_name)

		data_set_id = f"{self.client_name}_GAnalytics_{view_id}"

		self.bq.check_or_create_dataset(data_set_id)
		self.bq.check_or_create_tables(ga.tables_with_schema, data_set_id)

		date_range = slice_date_on_period(date_from, date_to, 1)

		for date_from, date_to in date_range:
			for report in ga.report_dict:
				if report_type in report:
					metric_list = [re.sub('[_]', ':', field) for field in list(ga.report_dict[report]['metrics'].keys())]
					dimension_list = [re.sub('[_]', ':', field) for field in
										list(ga.report_dict[report]['dimensions'].keys())]

					report_data = ga.get_request(date_from, date_to, metric_list, dimension_list)
					columns = [re.sub('[:]', '_', field) for field in report_data[0]]

					report_data_df = pd.DataFrame(report_data[1], columns=columns)

					self.bq.data_to_insert(report_data_df, ga.integer_fields, ga.float_fields, ga.string_fields,
											data_set_id, f"{self.client_name}_GAnalytics_{report}")

	def get_yandex_report(self, client_login, access_token, date_from, date_to, period, report_type):

		"""

		:param report_type: "KEYWORD" or "CAMPAIGN" or "AD"

		"""
		client_login_re = re.sub('[.-]', '_', client_login)
		yandex = YandexDirectReports(access_token, client_login, self.client_name)

		data_set_id = f"{self.client_name}_YandexDirect_{client_login_re}"

		date_range = slice_date_on_period(date_from, date_to, period)

		self.bq.check_or_create_dataset(data_set_id)
		self.bq.check_or_create_tables(yandex.tables_with_schema, data_set_id)

		for report in yandex.report_dict:
			if report_type in report:
				for date_from, date_to in date_range:
					report_data = yandex.get_report(f"{report}",
													f"{report} {datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}",
													date_from, date_to)
					report_data_split = report_data.text.split('\n')
					data = [x.split('\t') for x in report_data_split]
					stat = pd.DataFrame(data[1:-1], columns=data[:1][0])

					self.bq.data_to_insert(stat, yandex.integer_fields, yandex.float_fields, yandex.string_fields,
										data_set_id, f"{self.client_name}_YandexDirect_{report}")

	def get_mytarget_report(self, access_token, date_from, date_to, period, client_login):
		mt = MyTarget(access_token, self.client_name)

		date_range = slice_date_on_period(date_from, date_to, period)

		data_set_id = f"{self.client_name}_MyTarget_{client_login}"

		self.bq.check_or_create_dataset(data_set_id)
		self.bq.check_or_create_tables(mt.tables_with_schema, data_set_id)

		campaigns = mt.get_campaigns()
		campaigns_df = pd.DataFrame(campaigns)
		self.bq.insert_difference(campaigns_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
							f"{self.client_name}_MyTarget_CAMPAIGNS", 'id', 'id')

		ads = mt.get_ads()
		ads_df = pd.DataFrame(ads)
		self.bq.insert_difference(ads_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
							f"{self.client_name}_MyTarget_ADS", 'id', 'id')

		for date_from, date_to in date_range:
			stat_banner = mt.get_banner_stat(date_from, date_to)
			if stat_banner == []:
				continue
			stat_banner_df = pd.DataFrame(stat_banner)
			self.bq.data_to_insert(stat_banner_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
							f"{self.client_name}_MyTarget_BANNER_STAT")

			stat_campaigns = mt.get_campaigns_stat(date_from, date_to)
			stat_campaigns_df = pd.DataFrame(stat_campaigns)
			self.bq.data_to_insert(stat_campaigns_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
							f"{self.client_name}_MyTarget_CAMPAIGN_STAT")

	def get_vkontakte_report(self):
		pass

	def upload_data_to_analytics(self, data_frame_to_insert, account_id, web_property_id, custom_data_source_id,
								file_name, path_to_csv):

		ga_upload = GAnalyticsUpload(self.path_to_ga, account_id, web_property_id, custom_data_source_id)
		status, message = ga_upload.upload_data(data_frame_to_insert, path_to_csv, file_name)
		return status, message

	def get_yandex_direct_objects(self):
		pass
