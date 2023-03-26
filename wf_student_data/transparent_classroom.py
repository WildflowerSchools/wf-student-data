import wf_student_data.utils as utils
import requests
import pandas as pd
import numpy as np
import tqdm
import tqdm.notebook
import json
import time
import os
import logging

logger = logging.getLogger(__name__)

class TransparentClassroomClient:
    def __init__(
        self,
        username=None,
        password=None,
        api_token=None,
        url_base=None
    ):
        self.username = username
        self.password = password
        self.api_token = api_token
        self.url_base = url_base
        if self.url_base is None:
            self.url_base = 'https://www.transparentclassroom.com/api/v1/'
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
            # Try to add more detailed error info from HTTP response
            try:
                if r.json().get('errors') is not None:
                    error_message += '\n{}'.format(json.dumps(r.json().get('errors'), indent=2))
            except:
                pass
            raise Exception(error_message)
        return r.json()

    def fetch_classroom_child_data(
        self,
        session_ids=None,
        progress_bar=False,
        notebook=False,
        delay = None
    ):
        # If session IDs are not specified, fetch all session IDs
        # Each session ID is a tuple consisting of a TC school ID and a TC session ID
        if session_ids is None:
            session_ids = self.fetch_session_ids(
                progress_bar=progress_bar,
                notebook=notebook
            )
        # Wrap the iterator in the appropriate progress bar if requested
        if progress_bar:
            if notebook:
                session_id_iterator = tqdm.notebook.tqdm(session_ids)
            else:
                session_id_iterator = tqdm.tqdm(session_ids)
        else:
            session_id_iterator = session_ids
        classrooms_children_dfs = list()
        logger.info('Fetching classroom-child mapping from Transparent Classroom for {} sessions'.format(
            len(session_ids)
        ))
        if delay is not None:
            logger.info('Using delay of {} seconds between requests to avoid rate limit errors from TC API'.format(
                delay
            ))
        for school_id, session_id in session_id_iterator:
            classrooms_children_session = self.fetch_classroom_child_data_session(
                school_id=school_id,
                session_id=session_id
            )
            classrooms_children_dfs.append(classrooms_children_session)
            # Optional delay to avoid rate limit errors from TC API
            if delay is not None:
                time.sleep(delay)
        classrooms_children = (
            pd.concat(classrooms_children_dfs)
            .sort_index()
        )
        return classrooms_children

    def fetch_classroom_child_data_session(
        self,
        school_id,
        session_id        
    ):
        classrooms_children_session_list = self.request(
            'children.json',
            params={'session_id': session_id},
            school_id=school_id
        )
        if len(classrooms_children_session_list) == 0:
            return pd.DataFrame()
        # Classroom IDs are stored in TC as a list for each
        # school/session/child. We want to turn this into a one-to-many map from
        # classrooms to children
        classrooms_children_session = (
            pd.DataFrame(classrooms_children_session_list)
            .assign(school_id=school_id)
            .assign(session_id=session_id)
            .rename(columns={
                'id': 'child_id'
            })
            .reindex(columns=[
                'school_id',
                'session_id',
                'classroom_ids',
                'child_id'
            ])
            .dropna(subset=['classroom_ids'])
            .explode('classroom_ids')
            .rename(columns={
                'classroom_ids': 'classroom_id'
            })
            .set_index([
                'school_id',
                'session_id',
                'classroom_id',
                'child_id'
            ])
            .sort_index()
        )
        return classrooms_children_session
        
    def fetch_child_data(
        self,
        school_ids=None,
        progress_bar=False,
        notebook=False,
        delay = None
    ):
        # If school IDs are not specified, fetch all school IDs
        if school_ids is None:
            school_ids = self.fetch_school_ids()
        # Wrap the iterator in the appropriate progress bar if requested
        if progress_bar:
            if notebook:
                school_id_iterator = tqdm.notebook.tqdm(school_ids)
            else:
                school_id_iterator = tqdm.tqdm(school_ids)
        else:
            school_id_iterator = school_ids
        # We create two tables from TC's child data: a table of child data and a
        # one-to-many map of children to parents. 
        children_dfs = list()
        children_parents_dfs=list()
        logger.info('Fetching child data and child-parent mapping from Transparent Classroom for {} schools'.format(
            len(school_ids)
        ))
        if delay is not None:
            logger.info('Using delay of {} seconds between requests to avoid rate limit errors from TC API'.format(
                delay
            ))
        for school_id in school_id_iterator:
            children_school, children_parents_school = self.fetch_child_data_school(
                school_id=school_id
            )
            children_dfs.append(children_school)
            children_parents_dfs.append(children_parents_school)
            # Optional delay to avoid rate limit errors from TC API
            if delay is not None:
                time.sleep(delay)
        children = (
            pd.concat(children_dfs)
            .sort_index()
        )
        children_parents = (
            pd.concat(children_parents_dfs)
            .sort_index()
        )
        return children, children_parents

    def fetch_child_data_school(
        self,
        school_id
    ):
        children_school_list = self.request(
            'children.json',
            params={'session_id': 'all'},
            school_id=school_id
        )
        if len(children_school_list) == 0:
            logger.warning('School {} has zero children'.format(school_id))
            return pd.DataFrame(), pd.DataFrame()
        # First, we extract the child-level data, excluding the child-parent mapping
        children_school = (
            pd.DataFrame(children_school_list)
            .assign(school_id=school_id)
            .rename(columns={'id': 'child_id'})
            .reindex(columns=[
                'school_id',
                'child_id',
                'first_name',
                'middle_name',
                'last_name',
                'birth_date',
                'gender',
                'dominant_language',
                'ethnicity',
                'household_income',
                'student_id',
                'grade',
                'program',
                'first_day',
                'last_day',
                'exit_reason',
                'current_child'        
            ])
            .set_index(['school_id', 'child_id'])
            .sort_index()
        )
        children_school['birth_date'] = children_school['birth_date'].apply(utils.to_date)
        children_school['first_day'] = children_school['first_day'].apply(utils.to_date)
        children_school['last_day'] = children_school['last_day'].apply(utils.to_date)
        children_school['ethnicity'] = children_school['ethnicity'].replace({np.nan: None})
        # For each child, we want to know whether they are current in the TC
        # system. The only way to do this is to pull again with the only_current
        # parameter enabled and compare the lists
        children_school_current_list = self.request(
            'children.json',
            params={
                'session_id': 'all',
                'only_current': 'true'
            },
            school_id=school_id
        )
        if len(children_school_current_list) > 0:
            current_child_ids = {current_child['id'] for current_child in children_school_current_list}
            children_school['current_child'] = children_school.index.get_level_values('child_id').isin(current_child_ids)
        else:
            logger.warning('School {} has zero current children'.format(school_id))
            children_school['current_child'] = False
        # Second, we extract the child-parent mapping
        children_parents_school = (
            pd.DataFrame(children_school_list)
            .assign(school_id=school_id)
            .rename(columns={'id': 'child_id'})
            .reindex(columns=[
                'school_id',
                'child_id',
                'parent_ids',
            ])
            .dropna(subset=['parent_ids'])
            .explode('parent_ids')
            .rename(columns={'parent_ids': 'parent_id'})
            .set_index([
                'school_id',
                'child_id',
                'parent_id',        
            ])
            .sort_index()
        )
        if len(children_parents_school) == 0:
            logger.warning('School {} has children but zero parents'.format(
                school_id
            ))
        return children_school, children_parents_school
     
    def fetch_user_data(
        self,
        school_ids=None,
        progress_bar=False,
        notebook=False,
        delay = None
    ):
        # If school IDs are not specified, fetch all school IDs
        if school_ids is None:
            school_ids = self.fetch_school_ids()
        # Wrap the iterator in the appropriate progress bar if requested
        if progress_bar:
            if notebook:
                school_id_iterator = tqdm.notebook.tqdm(school_ids)
            else:
                school_id_iterator = tqdm.tqdm(school_ids)
        else:
            school_id_iterator = school_ids
        users_dfs = list()
        logger.info('Fetching user data from Transparent Classroom for {} schools'.format(
            len(school_ids)
        ))
        if delay is not None:
            logger.info('Using delay of {} seconds between requests to avoid rate limit errors from TC API'.format(
                delay
            ))
        for school_id in school_id_iterator:
            users_school = self.fetch_user_data_school(
                school_id=school_id
            )
            users_dfs.append(users_school)
            # Optional delay to avoid rate limit errors from TC API
            if delay is not None:
                time.sleep(delay)
        users = (
            pd.concat(users_dfs)
            .sort_index()
        )
        return users

    def fetch_user_data_school(
        self,
        school_id
    ):
        users_school_list = self.request(
            'users.json',
            params=None,
            school_id=school_id
        )
        if len(users_school_list) == 0:
            logger.warning('School {} has zero users'.format(school_id))
            return pd.DataFrame()
        users_school = (
            pd.DataFrame(users_school_list)
            .assign(school_id=school_id)
            .rename(columns={'id': 'user_id'})
            .reindex(columns=[
                'school_id',
                'user_id',
                'first_name',
                'last_name',
                'email',
                'roles',
                'inactive',
                'type'
            ])
            .set_index([
                'school_id',
                'user_id'
            ])
            .sort_index()
        )
        return users_school

    def fetch_session_ids(
        progress_bar=False,
        notebook=False
    ):
        sessions = self.fetch_session_data(
            school_ids=None,
            progress_bar=progress_bar,
            notebook=notebook
        )
        session_ids = sessions.index.tolist()
        return session_ids
    
    def fetch_session_data(
        self,
        school_ids=None,
        progress_bar=False,
        notebook=False,
        delay = None
    ):
        # If school IDs are not specified, fetch all school IDs
        if school_ids is None:
            school_ids = self.fetch_school_ids()
        # Wrap the iterator in the appropriate progress bar if requested
        if progress_bar:
            if notebook:
                school_id_iterator = tqdm.notebook.tqdm(school_ids)
            else:
                school_id_iterator = tqdm.tqdm(school_ids)
        else:
            school_id_iterator = school_ids
        sessions_dfs = list()
        logger.info('Fetching session data from Transparent Classroom for {} schools'.format(
            len(school_ids)
        ))
        if delay is not None:
            logger.info('Using delay of {} seconds between requests to avoid rate limit errors from TC API'.format(
                delay
            ))
        for school_id in school_id_iterator:
            sessions_school = self.fetch_session_data_school(
                school_id=school_id
            )
            sessions_dfs.append(sessions_school)
            # Optional delay to avoid rate limit errors from TC API
            if delay is not None:
                time.sleep(delay)
        sessions = (
            pd.concat(sessions_dfs)
            .sort_index()
        )
        return sessions

    def fetch_session_data_school(
        self,
        school_id
    ):
        sessions_school_list = self.request(
            'sessions.json',
            params=None,
            school_id=school_id
        )
        if len(sessions_school_list) == 0:
            logger.warning('School {} has zero sessions'.format(
                school_id
            ))
            return pd.DataFrame()
        sessions_school = (
            pd.DataFrame(sessions_school_list)
            .assign(school_id=school_id)
            .rename(columns={'id': 'session_id'})
            .reindex(columns=[
                'school_id',
                'session_id',
                'name',
                'start_date',
                'stop_date',
                'current',
                'inactive',
                'children'        
            ])
            .set_index([
                'school_id',
                'session_id'
            ])
            .sort_index()
        )
        sessions_school['start_date'] = sessions_school['start_date'].apply(utils.to_date)
        sessions_school['stop_date'] = sessions_school['stop_date'].apply(utils.to_date)
        return sessions_school

    def fetch_classroom_data(
        self,
        school_ids=None,
        progress_bar=False,
        notebook=False,
        delay = None
    ):
        # If school IDs are not specified, fetch all school IDs
        if school_ids is None:
            school_ids = self.fetch_school_ids()
        # Wrap the iterator in the appropriate progress bar if requested
        if progress_bar:
            if notebook:
                school_id_iterator = tqdm.notebook.tqdm(school_ids)
            else:
                school_id_iterator = tqdm.tqdm(school_ids)
        else:
            school_id_iterator = school_ids
        classrooms_dfs = list()
        logger.info('Fetching classroom data from Transparent Classroom for {} schools'.format(
            len(school_ids)
        ))
        if delay is not None:
            logger.info('Using delay of {} seconds between requests to avoid rate limit errors from TC API'.format(
                delay
            ))
        for school_id in school_id_iterator:
            classrooms_school = self.fetch_classroom_data_school(
                school_id=school_id
            )
            classrooms_dfs.append(classrooms_school)
            # Optional delay to avoid rate limit errors from TC API
            if delay is not None:
                time.sleep(delay)
        classrooms = (
            pd.concat(classrooms_dfs)
            .sort_index()
        )
        return classrooms

    def fetch_classroom_data_school(
        self,
        school_id
    ):
        classrooms_school_list = self.request(
            'classrooms.json',
            params={
                'show_inactive': 'true'
            },
            school_id=school_id
        )
        if len(classrooms_school_list) == 0:
            logger.warning('School {} has zero classrooms'.format(
                school_id
            ))
            return pd.DataFrame()
        classrooms_school = (
            pd.DataFrame(classrooms_school_list)
            .assign(school_id=school_id)
            .rename(columns={'id': 'classroom_id'})
            .reindex(columns=[
                'school_id',
                'classroom_id',
                'name',
                'lesson_set_id',
                'level',
                'active'
            ])
            .set_index([
                'school_id',
                'classroom_id'
            ])
            .sort_index()
        )
        return classrooms_school
    
    def fetch_school_ids(self):
        schools = self.fetch_school_data()
        school_ids = schools.index.tolist()
        return school_ids
    
    def fetch_school_data(self):
        logger.info('Fetching school data from Transparent Classroom')
        schools_list = self.request(
            'schools.json',
            params=None,
            school_id=None
        )
        schools = (
            pd.DataFrame(schools_list)
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
            .sort_index()
        )
        return schools
