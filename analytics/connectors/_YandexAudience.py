import requests, json


class YandexAudience:
    def __init__(self, access_token):
        self.access_token = access_token
        self.url = "https://api-audience.yandex.ru/v1/management/"

    def _create_headers(self):
        headers = {"Authorization": "Bearer " + self.access_token, "name": "file"}
        return headers

    def _create_body(self, segment_params):
        body = {
            "segment": segment_params
        }
        jBody = json.dumps(body, ensure_ascii=False).encode('utf8')
        return jBody

    def _check_errors(self, response):
        if response.status_code == 200:
            return response.json()
        else:
            errors = response.content.decode("utf8")
            raise Exception(errors)

    def _create_response_by_crm_data(self, path_to_file, file_name):
        headers = self._create_headers()
        files = {'file': open(path_to_file + file_name, 'rb')}
        response = requests.post(self.url + "segments/upload_csv_file", files=files, headers=headers)
        response = self._check_errors(response)
        request_id = response['segment']['id']
        return request_id

    def create_audience_by_crm_data(self, path_to_file, file_name, segment_name, hashed=False):
        headers = self._create_headers()
        request_id = self._create_response_by_crm_data(path_to_file, file_name)
        segment_params = {"id": request_id, "name": segment_name, "hashed": hashed, "content_type": "crm"}
        body = self._create_body(segment_params)
        response = requests.post(self.url + f"segment/{request_id}/confirm", body, headers=headers)
        response = self._check_errors(response)
        return response['segment']['id'], response['segment']['name'], response['segment']['status']
