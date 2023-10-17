# Imports
import pandas as pd
import numpy as np
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


# Functions
def get_access_token(user: str, password: str):
    """POST API and return access token

    Args:
        user (str): User
        password (str): Password
        env (str): [sistema, staging]

    Raises:
        Exception: Usuário/Senha incorreto. Favor verificar!

    Returns:
        str: Access Token
    """
    user = login_user
    password = login_pwd

    headers_token = {
        'accept': '*/*',
        'Content-Type': 'application/json',
    }

    # Set headers content
    data_token = (
        '{"username":' + f'"{user}",'
        f'"password":"{password}",'
        '"refresh_token":""}'
    )

    # POST for token
    response = requests.post(
        'https://sistema.solvis.net.br/api/v1/oauth/token',
        headers=headers_token,
        data=data_token
    )

    # Get access token
    try:
        access_token = response.json()['access_token']
        return access_token
    except KeyError:
        raise Exception('Usuário/Senha incorreto. Favor verificar!')


def request_api(access_token: str, page: int):
    """ Get API and return JSON

    Args:
    Returns:
        json: JSON
    """
    # Set headers
    headers = {
        'accept': '*/*',
        'Authorization': f'Bearer {access_token}',
    }

    # Create session
    session = requests.Session()

    response_survey = session.get(
        f'https://sistema.solvis.net.br/api/v1/surveys/'
        f'{survey_id}/evaluations?search_date_scope={scope}&'
        f'start_date={start_date}&'
        f'end_date={end_date}&'
        f'page={str(page)}&'
        'per_page=100',
        headers=headers,
    )

    return response_survey


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
        count = len(answers)

        if answers:
            for idx in range(len(answers)):
                questions = answers[idx].pop('formatted_answers')
                df_answers = pd.json_normalize(answers[idx])
                df_answers.set_index(pd.Index([idx]), inplace=True)

                for i in range(len(questions)):
                    # NPS question type
                    if questions[i]['answer_type'] == 'NPS':
                        try:
                            # df_answers['nps_original'] = np.nan
                            df_answers['nps_original'] = None
                            # df_answers['nps_original'] = df_answers['nps_original'].astype('float')
                            df_answers.loc[idx, 'nps_original'] = questions[i]['answers'][0]['answer_value']
                        except KeyError:
                            pass
                            # df_answers.loc[idx, 'nps_original'] = np.nan
                        try:
                            # df_answers['nps_tipo'] = ''
                            df_answers['nps_tipo'] = None
                            df_answers.loc[idx, 'nps_tipo'] = questions[i]['answers'][0]['answer_text']
                        except KeyError:
                            pass
                            # df_answers.loc[idx, 'nps_tipo'] = np.nan

                    # SCALE question type
                    elif questions[i]['answer_type'] == 'Scale':
                        for choice in range(len(questions[i]['answers'])):
                            column_name = questions[i]['answers'][choice]['question_text']
                            try:
                                # df_answers[column_name] = ''
                                df_answers[column_name] = None
                                df_answers.loc[idx, column_name] = questions[i]['answers'][choice]['choice_text']
                            except KeyError:
                                pass
                                # df_answers.loc[idx, column_name] = np.nan
                            try:
                                # df_answers[f'{column_name}_valor'] = np.nan
                                df_answers[f'{column_name}_valor'] = None
                                df_answers.loc[idx, f'{column_name}_valor'] = float((questions[i]['answers'][choice]['choice_value']))
                            except (KeyError, TypeError):
                                pass
                                # df_answers.loc[idx, f'{column_name}_valor'] = np.nan

                    # TEXT question type
                    elif questions[i]['answer_type'] in ('Text', 'Short Text'):
                        for choice in range(len(questions[i]['answers'])):
                            column_name = questions[i]['answers'][choice]['question_text']
                            try:
                                # df_answers[column_name] = ''
                                df_answers[column_name] = None
                                df_answers.loc[idx, column_name] = questions[i]['answers'][choice]['choice_value']
                            except KeyError:
                                pass
                                # df_answers.loc[idx, column_name] = np.nan

                    # MULTIPLE CHOICE question type
                    elif questions[i]['answer_type'] == 'Multiple Choice':
                        for choice in range(len(questions[i]['answers'])):
                            column_name = questions[i]['answers'][choice]['question_text']
                            try:
                                # df_answers[column_name] = ''
                                df_answers[column_name] = None
                                df_answers.loc[idx, column_name] = questions[i]['answers'][choice]['choice_text']
                            except KeyError:
                                pass
                                # df_answers.loc[idx, column_name] = np.nan
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
                            # df_answers[column_name] = ''
                            df_answers[column_name] = None
                            df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                        except KeyError:
                            pass
                            # df_answers.loc[idx, column_name] = np.nan

                    # CPF question type
                    elif questions[i]['answer_type'] == 'CPF':
                        column_name = questions[i]['answers'][0][0]['question_text']
                        try:
                            # df_answers[column_name] = ''
                            df_answers[column_name] = None
                            df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                        except KeyError:
                            pass
                            # df_answers.loc[idx, column_name] = np.nan

                    # CNPJ question type
                    elif questions[i]['answer_type'] == 'CNPJ':
                        column_name = questions[i]['answers'][0][0]['question_text']
                        try:
                            # df_answers[column_name] = ''
                            df_answers[column_name] = None
                            df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                        except KeyError:
                            pass
                            # df_answers.loc[idx, column_name] = np.nan

                    # EMAIL question type
                    elif questions[i]['answer_type'] == 'Email':
                        column_name = questions[i]['answers'][0][0]['question_text']
                        try:
                            # df_answers[column_name] = ''
                            df_answers[column_name] = None
                            df_answers.loc[idx, column_name] = questions[i]['answers'][0][0]['choice_text']
                        except KeyError:
                            pass
                            # df_answers.loc[idx, column_name] = np.nan

                df = pd.concat([df, df_answers], ignore_index=False)

            print(f'Página: {page} - OK!')
            page += 1
            total = total + count
        else:
            print('Fim da exportação!')
            print(f'Total de avaliações: {total}')
            break


# Check if dataframe is not empty
if df.shape[0] > 0:
    df.to_csv(f"resultados_{survey_id}_{start_date.split('T')[0]}-a-{end_date.split('T')[0]}.csv", index=False)
else:
    print('Sem respostas no período!')
print('FIM!')
