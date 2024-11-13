# Imports
from datetime import datetime
import pandas as pd
import requests
from json import JSONDecodeError


# Input params
survey_id = 'XXXXXXXXXX'  # ID DA PESQUISA
start_date = 'XXXXXXXXXX'  # DATA INICIAL NO FORMATO "YYYY-MM-DDTHH:MM:SS" (O "T" É OBRIGATÓRIO)
end_date = 'XXXXXXXXXX'  # DATA INICIAL NO FORMATO "YYYY-MM-DDTHH:MM:SS" (O "T" É OBRIGATÓRIO)
scope = 'answered_at'  # ESCOPO DA CONSULTA, SE É PELA DATA DE RESPOSTA NO TOTEM ("answered_at") OU SE É PELA DATA DA INSERÇÃO NO BANCO DE DADOS ("received_at")


# Login & Password
login_user = 'XXXXXXXXXX'  # USUÁRIO DA API
login_pwd = 'XXXXXXXXXX'  # SENHA DO USUÁRIO DA API


class GetEvaluations:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.access_token = None

    def get_evaluations(
        self,
        user: str | Any,
        password: str | Any,
        survey_id: str | Any,
        start_datetime: str | Any,
        end_datetime: str | Any,
        per_page=10000,
        env='sistema',
        scope='answered_at',
    ) -> list[dict]:
        """Get evaluations

        Args:
            user (str): API username
            password (str): API password
            survey_id (str): Survey ID
            start_datetime (str): Start date (format YYYY-MM-DDTHH:MM:SS)
            end_datetime (str): End date (format YYYY-MM-DDTHH:MM:SS)
            per_page (int): Total evaluations per page (max: 10000)
            env (str): [sistema, staging]
            scope (str): [answered_at, received_at]

        Returns:
            dict: Pandas Dataframe
        """

        self.user = user
        self.password = password
        self.survey_id = survey_id
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

        # Check for valid period
        date_start = datetime.strptime(self.start_datetime.split('T')[0], '%Y-%m-%d')
        date_end = datetime.strptime(self.end_datetime.split('T')[0], '%Y-%m-%d')
        delta = date_end - date_start

        if delta.days > 31:
            raise Exception('O período selecionado excede o limite máximo de 31 dias!')

        def request_api():
            """Get API and return JSON

            Args:
            Returns:
                json: JSON
            """
            headers_token = {
                'accept': '*/*',
                'Content-Type': 'application/json',
            }

            # Set headers content
            data_token = (
                '{"username":' + f'"{self.user}",'
                f'"password":"{self.password}",'
                '"refresh_token":""}'
            )

            # POST for token
            response = requests.post(
                f'https://{env}.solvis.net.br/api/v1/oauth/token',
                headers=headers_token,
                data=data_token,
            )

            # Get access token
            try:
                self.access_token = response.json()['access_token']
            except KeyError:
                raise Exception('Usuário/Senha incorreto. Favor verificar!')

            # Set headers
            headers = {
                'accept': '*/*',
                'Authorization': f'Bearer {self.access_token}',
            }

            response_survey = self.session.get(
                f'https://{env}.solvis.net.br/api/v1/surveys/'
                f'{self.survey_id}/evaluations?search_date_scope={scope}&'
                f'start_date={self.start_datetime}&'
                f'end_date={self.end_datetime}&'
                f'page={str(self.page)}&'
                f'per_page={per_page}',
                headers=headers,
            )

            return response_survey

        # Set default values
        self.page = 1
        total = 0
        evaluations = []

        print('Iniciando exportação de avaliações...')
        time_start = time.perf_counter()
        while True:
            try:
                response = request_api()
            except (ConnectionError, ConnectionAbortedError) as error:
                raise Exception(error)

            if response.status_code != 200:
                raise Exception(f'Erro: {response.status_code}')
            else:
                try:
                    json = response.json()
                except JSONDecodeError as error:
                    raise Exception(error)

                try:
                    if json['error']:
                        raise Exception(json['error'])
                except KeyError:
                    pass
                if json['data']:
                    answers = json['data']
                    evaluations.append(answers)
                    count = len(answers)

                    print(f'Página: {self.page} - OK!')
                    self.page += 1
                    total = total + count
                else:
                    time_end = time.perf_counter()
                    time_final = round(time_end - time_start, 2)
                    print('Fim da exportação!')
                    print(f'Total de avaliações: {total}')
                    print(f'Tempo total: {time_final} segundo(s)')
                    break

        # Close connection
        self.session.close()

        return evaluations


class DataProcessing:
    def data_processing(self, evaluations: list) -> pd.DataFrame:
        """Receives a list of dictionaries and return a processed dataframe

        Args:
            evaluations (list): A list of dictionaries

        Returns:
            pd.DataFrame: Processed dataframe
        """
        print('Iniciando processamento de dados...')
        records = []

        time_start = time.perf_counter()
        if evaluations:
            for page, eval_page in enumerate(evaluations):
                for idx, evaluation in enumerate(eval_page):
                    questions = evaluation.pop('formatted_answers')
                    record = {
                        **pd.json_normalize(evaluation, sep='__').iloc[0].to_dict()
                    }

                    for question in questions:
                        answer_type = question['answer_type']
                        for answer in question['answers']:
                            if isinstance(answer, dict):
                                base_key = answer['question_text']
                            elif isinstance(answer, list):
                                answer = answer[0]
                                base_key = answer['question_text']

                            if answer_type == 'NPS':
                                record[base_key] = answer.get('answer_text', None)
                                record[f'{base_key}_valor'] = answer.get(
                                    'answer_value', None
                                )

                            elif answer_type == 'Scale':
                                record[base_key] = answer.get('choice_text', None)
                                record[f'{base_key}_valor'] = (
                                    float(answer.get('choice_value', 0))
                                    if answer.get('choice_value') is not None
                                    else None
                                )

                            elif answer_type == 'Multiple Choice':
                                for field in answer:
                                    if 'additional_field_answer' in field:
                                        key = f"{answer['question_text']}_{answer['choice_text']}"
                                        additional_field_key = answer[
                                            'additional_field'
                                        ]
                                        additional_field_value = answer[
                                            'additional_field_answer'
                                        ]
                                        record[f'{key}_{additional_field_key}'] = (
                                            additional_field_value
                                        )
                                    else:
                                        record[base_key] = answer.get(
                                            'choice_text', None
                                        )

                            elif answer_type in ['Text', 'Short Text']:
                                record[f'{base_key}'] = answer.get('choice_value', None)

                            elif answer_type in ['Phone', 'CPF', 'CNPJ', 'Email']:
                                record[f'{base_key}'] = answer.get('choice_text', None)

                            elif answer_type == 'Multiple Response':
                                for question_text, choices in question[
                                    'answers'
                                ].items():
                                    for choice in choices:
                                        key = f"{question_text}_{choice['choice_text']}"
                                        record[key] = 1
                                        if (
                                            'additional_field' in choice
                                            and 'additional_field_answer' in choice
                                        ):
                                            additional_field_key = choice[
                                                'additional_field'
                                            ]
                                            additional_field_value = choice[
                                                'additional_field_answer'
                                            ]
                                            record[f'{key}_{additional_field_key}'] = (
                                                additional_field_value
                                            )

                    records.append(record)
                print(f'Página: {page + 1} - OK!')

            time_end = time.perf_counter()
            print('Fim do processamento de dados!')
            print(f'Tempo total: {round(time_end - time_start, 2)} segundo(s)')

        df_final = pd.DataFrame(records)
        return df_final


# Load module
api = GetEvaluations()
data = DataProcessing()


# Request evaluations
evaluations = api.get_evaluations(
    user=login_user,
    password=login_pwd,
    survey_id=survey_id,
    start_datetime=start_date,
    end_datetime=end_date,
)

# Data processing
df = data.data_processing(evaluations)


# Realizar a exportação desse dataframe (df) da maneira que desejar.
