# Imports
import pandas as pd
import numpy as np
import requests
import time
from json import JSONDecodeError


# Input params
survey_id = 'XXXXX'  # ID DA PESQUISA
start_date = 'XXXXX'  # DATA INICIAL NO FORMATO "YYYY-MM-DDTHH:MM:SS" (O "T" É OBRIGATÓRIO)
end_date = 'XXXXX'  # DATA INICIAL NO FORMATO "YYYY-MM-DDTHH:MM:SS" (O "T" É OBRIGATÓRIO)
scope = 'answered_at'  # ESCOPO DA CONSULTA, SE É PELA DATA DE RESPOSTA NO TOTEM ("answered_at") OU SE É PELA DATA DA INSERÇÃO NO BANCO DE DADOS ("received_at")


# Login & Password
login_user = 'XXXXX'  # USUÁRIO DA API
login_pwd = 'XXXXX'  # SENHA DO USUÁRIO DA API


# Functions
def get_access_token(login: str, password: str):
    """POST API and return access token

    Args:
        login (str): Login (login_user)
        password (str): Password (login_pwd)

    Raises:
        Exception: Login/Senha incorreto. Favor verificar!

    Returns:
        str: Access Token
    """
    headers_token = {
        'accept': '*/*',
        'Content-Type': 'application/json',
    }

    # Set headers content
    data_token = (
        '{"username":' + f'"{login}",'
        f'"password":"{password}",'
        '"refresh_token":""}'
    )

    # POST for token
    response = requests.post(
        "https://sistema.solvis.net.br/api/v1/oauth/token",
        headers=headers_token,
        data=data_token
    )

    # Get access token
    try:
        access_token = response.json()['access_token']
        return access_token
    except KeyError:
        raise Exception('Login/Senha incorreto. Favor verificar!')


def request_api(token: str, page: int):
    """ GET API and return JSON

    Args:
        token (str): Access token
        page (int): Page number

    Returns:
        str: JSON
    """
    # Set headers
    headers = {
        'accept': '*/*',
        'Authorization': f'Bearer {token}',
    }

    # # Create session
    # session = requests.Session()

    response_survey = session.get(
        f'https://sistema.solvis.net.br/api/v1/surveys/{survey_id}/evaluations?'
        f'search_date_scope={scope}&'
        f'start_date={start_date}&'
        f'end_date={end_date}&'
        f'page={str(page)}',
        headers=headers,
    )

    return response_survey


# Create session
session = requests.Session()


# Create empty dataframe
df = pd.DataFrame()


# Get token
access_token = get_access_token(login_user, login_pwd)


# Request API
page = 1
total = 0

while True:
    try:
        response = request_api(access_token, page)
    except (ConnectionError, ConnectionAbortedError) as error:
        print(error)

    if response.status_code == 401:
        print('Access token inválido.')
        access_token = get_access_token(login_user, login_pwd)
        session = requests.Session()

    elif response.status_code == 500:
        raise Exception('Servidor indisponível.')

    elif response.status_code == 200:
        try:
            json = response.json()
        except JSONDecodeError as error:
            print(error)

        answers = json['data']

        if answers:
            for idx in range(len(answers)):
                questions = answers[idx].pop('formatted_answers')
                df_answers = pd.json_normalize(answers[idx])
                df_answers.set_index(pd.Index([idx]), inplace=True)

                for i in range(len(questions)):
                    # NPS question type
                    if questions[i]['answer_type'] == 'NPS':
                        try:
                            df_answers.loc[idx, 'NPS_Original'] = questions[i]['answers'][0]['answer_value']
                        except KeyError:
                            df_answers.loc[idx, 'NPS_Original'] = np.nan
                        try:
                            df_answers.loc[idx, 'NPS_Tipo'] = questions[i]['answers'][0]['answer_text']
                        except KeyError:
                            df_answers.loc[idx, 'NPS_Tipo'] = np.nan

                    # SCALE question type
                    elif questions[i]['answer_type'] == 'Scale':
                        for choice in range(len(questions[i]['answers'])):
                            column_name = questions[i]['answers'][choice]['question_text']
                            try:
                                df_answers.loc[idx, column_name] = questions[i]['answers'][choice]['choice_text']
                            except KeyError:
                                df_answers.loc[idx, column_name] = np.nan
                            try:
                                df_answers.loc[idx, f'{column_name}_valor'] = int(float((questions[i]['answers'][choice]['choice_value'])))
                            except KeyError:
                                df_answers.loc[idx, f'{column_name}_valor'] = np.nan

                    # TEXT question type
                    elif questions[i]['answer_type'] in ('Text', 'Short Text'):
                        column_name = questions[i]['answers'][0]['question_text']
                        try:
                            df_answers.loc[idx, column_name] = questions[i]['answers'][0]['choice_value']
                        except KeyError:
                            df_answers.loc[idx, column_name] = np.nan

                    # MULTIPLE CHOICE question type
                    elif questions[i]['answer_type'] == 'Multiple Choice':
                        for choice in range(len(questions[i]['answers'])):
                            column_name = questions[i]['answers'][choice]['question_text']
                            try:
                                df_answers.loc[idx, column_name] = questions[i]['answers'][choice]['choice_text']
                            except KeyError:
                                df_answers.loc[idx, column_name] = np.nan
                            try:
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
                                        df_answers.loc[idx, f'{k}_{v[choice]["additional_field"].split(": ")[1][:-1]}'] = v[choice]['additional_field_answer']
                                    except KeyError:
                                        pass

                    # PHONE question type
                    elif questions[i]['answer_type'] == 'Phone':
                        column_name = questions[i]['answers'][0][0]['question_text']
                        try:
                            df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                        except KeyError:
                            df_answers.loc[idx, column_name] = np.nan

                    # CPF question type
                    elif questions[i]['answer_type'] == 'CPF':
                        column_name = questions[i]['answers'][0][0]['question_text']
                        try:
                            df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                        except KeyError:
                            df_answers.loc[idx, column_name] = np.nan

                    # CNPJ question type
                    elif questions[i]['answer_type'] == 'CNPJ':
                        column_name = questions[i]['answers'][0][0]['question_text']
                        try:
                            df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                        except KeyError:
                            df_answers.loc[idx, column_name] = np.nan

                    # EMAIL question type
                    elif questions[i]['answer_type'] == 'Email':
                        column_name = questions[i]['answers'][0][0]['question_text']
                        try:
                            df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                        except KeyError:
                            df_answers.loc[idx, column_name] = np.nan

                df = pd.concat([df, df_answers], ignore_index=False)

            print(f'Página: {page} - OK!')
            page += 1
        else:
            print('Fim da exportação!')
            break


# Check if dataframe is not empty
if df.shape[0] > 0:
    df.to_csv(f"resultados_{survey_id}_{start_date.split('T')[0]}-a-{end_date.split('T')[0]}.csv", index=False)
else:
    print('Sem respostas no período!')
print('FIM!')
