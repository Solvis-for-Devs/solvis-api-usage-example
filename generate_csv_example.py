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
    def get_evaluations(self, user: str, password: str, survey_id: str,
                        start_datetime: str, end_datetime: str, per_page=10000,
                        env='sistema', scope='answered_at') -> pd.DataFrame:
        """ Get evaluations

        Args:
            user (str): API username
            password (str): API password
            survey_id (str): Survey ID
            start_datetime (str): Start date (format YYYY-MM-DDTHH:MM:SS)
            end_datetime (str): End date (format YYYY-MM-DDTHH:MM:SS)
            per_page (int): Total evaluations per page (max: 100)
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
            """ Get API and return JSON

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
                data=data_token
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

            # Create session
            self.session = requests.Session()

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

        while True:
            try:
                time_start = time.perf_counter()
                response = request_api()
            except (ConnectionError) as error:
                raise Exception(error)

            if response.status_code == 400:
                raise Exception('Erro 400: Bad request.')

            elif response.status_code == 401:
                print('Erro 401: Token inválido.')

            elif response.status_code == 500:
                raise Exception('Erro 500: Servidor indisponível')

            elif response.status_code == 200:
                try:
                    json = response.json()
                except JSONDecodeError as error:
                    raise Exception(error)

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
                    print(f'Tempo total: {time_final}')
                    break

        return evaluations


class DataProcessing:
    def data_processing(self, evaluations: list) -> pd.DataFrame:
        # Create empty dataframe
        df_temp = pd.DataFrame()

        if evaluations:
            for page in range(len(evaluations)):
                for idx in range(len(evaluations[page])):
                    questions = evaluations[page][idx].pop('formatted_answers')
                    df_answers = pd.json_normalize(evaluations[page][idx])
                    df_answers.set_index(pd.Index([idx]), inplace=True)

                    for i in range(len(questions)):
                        # NPS question type
                        if questions[i]['answer_type'] == 'NPS':
                            for choice in range(len(questions[i]['answers'])):
                                column_name = questions[i]['answers'][choice]['question_text']
                                try:
                                    df_answers[column_name] = None
                                    df_answers.loc[idx, column_name] = questions[i]['answers'][0]['answer_text']
                                except KeyError:
                                    pass
                                try:
                                    df_answers[f'{column_name}_valor'] = None
                                    df_answers.loc[idx, f'{column_name}_valor'] = questions[i]['answers'][0]['answer_value']
                                except KeyError:
                                    pass

                        # SCALE question type
                        elif questions[i]['answer_type'] == 'Scale':
                            for choice in range(len(questions[i]['answers'])):
                                column_name = questions[i]['answers'][choice]['question_text']
                                try:
                                    df_answers[column_name] = None
                                    df_answers.loc[idx, column_name] = questions[i]['answers'][choice]['choice_text']
                                except KeyError:
                                    pass
                                try:
                                    df_answers[f'{column_name}_valor'] = None
                                    df_answers.loc[idx, f'{column_name}_valor'] = float((questions[i]['answers'][choice]['choice_value']))
                                except (KeyError, TypeError):
                                    pass

                        # TEXT question type
                        elif questions[i]['answer_type'] in ('Text', 'Short Text'):
                            for choice in range(len(questions[i]['answers'])):
                                column_name = questions[i]['answers'][choice]['question_text']
                                try:
                                    df_answers[column_name] = None
                                    df_answers.loc[idx, column_name] = questions[i]['answers'][choice]['choice_value']
                                except KeyError:
                                    pass

                        # MULTIPLE CHOICE question type
                        elif questions[i]['answer_type'] == 'Multiple Choice':
                            for choice in range(len(questions[i]['answers'])):
                                column_name = questions[i]['answers'][choice]['question_text']
                                try:
                                    df_answers[column_name] = None
                                    df_answers.loc[idx, column_name] = questions[i]['answers'][choice]['choice_text']
                                except KeyError:
                                    pass
                                try:
                                    df_answers[f'{column_name}_{questions[i]["answers"][choice]["additional_field"].split(": ")[1][:-1]}'] = questions[i]['answers'][choice]['additional_field_answer'] = ''
                                    df_answers.loc[idx, f'{column_name}_{questions[i]["answers"][choice]["additional_field"].split(": ")[1][:-1]}'] = questions[i]['answers'][choice]['additional_field_answer']
                                except KeyError:
                                    pass

                        # MULTIPLE RESPONSE question type
                        elif questions[i]['answer_type'] == 'Multiple Response':
                            for k, v in questions[i]['answers'].items():
                                for choice in range(len(v)):
                                    if v[choice]['choice_text'] is not None:
                                        try:
                                            df_answers.loc[idx, f'{k}_{v[choice]["choice_text"]}'] = 1
                                        except KeyError:
                                            df_answers.loc[idx, f'{k}_{v[choice]["choice_text"]}'] = 0
                                        try:
                                            df_answers[f'{k}_{v[choice]["additional_field"].split(": ")[1][:-1]}'] = v[choice]['additional_field_answer']
                                            df_answers.loc[idx, f'{k}_{v[choice]["additional_field"].split(": ")[1][:-1]}'] = v[choice]['additional_field_answer']
                                        except KeyError:
                                            pass

                        # PHONE question type
                        elif questions[i]['answer_type'] == 'Phone':
                            column_name = questions[i]['answers'][0][0]['question_text']
                            try:
                                df_answers[column_name] = None
                                df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                            except KeyError:
                                pass

                        # CPF question type
                        elif questions[i]['answer_type'] == 'CPF':
                            column_name = questions[i]['answers'][0][0]['question_text']
                            try:
                                df_answers[column_name] = None
                                df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                            except KeyError:
                                pass

                        # CNPJ question type
                        elif questions[i]['answer_type'] == 'CNPJ':
                            column_name = questions[i]['answers'][0][0]['question_text']
                            try:
                                df_answers[column_name] = None
                                df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                            except KeyError:
                                pass

                        # EMAIL question type
                        elif questions[i]['answer_type'] == 'Email':
                            column_name = questions[i]['answers'][0][0]['question_text']
                            try:
                                df_answers[column_name] = None
                                df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                            except KeyError:
                                pass

                    df_temp = pd.concat([df_temp, df_answers], ignore_index=False)
                print(f'Página: {page} - OK!')
            print('Fim do processamento de dados!')

        return df_temp


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


# Export dataframe
if df.shape[0] > 0:
    df.to_csv(f'resultados_{survey_id}_{start_date.split('T')[0]}-a-{end_date.split('T')[0]}.csv', index=False)
else:
    print('Sem respostas no período!')
print('FIM!')
