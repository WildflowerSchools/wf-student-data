import requests
import pandas as pd
import json
import os
import logging

logger = logging.getLogger(__name__)

class TransparentClassroomClient:
    def __init__(
        self,
        username=None,
        password=None,
        api_token=None,
        url_base='https://www.transparentclassroom.com/api/v1/'
    ):
        self.username = username
        self.password = password
        self.api_token = api_token
        self.url_base = url_base
        if self.api_token is None:
            self.api_token = os.getenv('TRANSPARENT_CLASSROOM_API_TOKEN')
        if self.api_token is None:
            logger.info('Transparent Classroom API token not specified. Attempting to generate token.')
            if self.username is None:
                self.username = os.getenv('TRANSPARENT_CLASSROOM_USERNAME')
            if self.username is None:
                raise ValueError('Transparent Classroom username not specified')
            if self.password is None:
                self.password = os.getenv('TRANSPARENT_CLASSROOM_PASSWORD')
            if self.password is None:
                raise ValueError('Transparent Classroom password not specified')
            json_output = self.request(
                'authenticate.json',
                auth=(self.username, self.password)
            )
            self.api_token = json_output['api_token']
    
    def fetch_school_data(
        self,
        parse=True
    ):
        schools = self.request(
            'schools.json',
            params=None,
            school_id=None
        )
        if parse:
            schools = parse_school_data(schools)
        return schools

    def request(
        self,
        endpoint,
        params=None,
        school_id=None,
        masquerade_id=None,
        auth=None
    ):
        headers = dict()
        if self.api_token is not None:
            headers['X-TransparentClassroomToken'] = self.api_token
        if school_id is not None:
            headers['X-TransparentClassroomSchoolId'] = str(school_id)
        if masquerade_id is not None:
            headers['X-TransparentClassroomMasqueradeId'] = str(masquerade_id)
        r = requests.get(
            '{}{}'.format(self.url_base, endpoint),
            params=params,
            headers=headers,
            auth=auth
        )
        if r.status_code != 200:
            error_message = 'Transparent Classroom request returned status code {}'.format(r.status_code)
            try:
                if r.json().get('errors') is not None:
                    error_message += '\n{}'.format(json.dumps(r.json().get('errors'), indent=2))
            except:
                pass
            raise Exception(error_message)
        return r.json()

def parse_school_data(
    schools
):
    schools = (
        pd.DataFrame(schools)
        .rename(columns={'id': 'school_id'})
        .reindex(columns=[
            'school_id',
            'name',
            'address',
            'phone',
            'time_zone'        
        ])
        .set_index([
            'school_id'
        ])
    )
    return schools
