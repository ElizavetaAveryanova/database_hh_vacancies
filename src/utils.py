import requests
from config_hh import hh_config
import psycopg2


def get_employers() -> list[dict]:
    """Получает с сайта hh.ru данные работодателей по их id """

    employers_list = []

    # перебираем id работодателей из списка выбранных
    for employer_id in hh_config.get("employer_list"):
        # формируем ссылку на страницу работодателя
        employer_hh_url = f"https://api.hh.ru/employers/{employer_id}"
        # делаем request запрос
        response = requests.get(employer_hh_url)
        if response.status_code == 200:
            # переводим полученные данные в json формат
            employer = response.json()
            # собираем данные по работодателям в список
            employers_list.append({"employer_id": employer["id"],
                                   "employer_title": employer["name"],
                                   "employer_url": employer["site_url"],
                                   "open_vacancies": employer["open_vacancies"]})
        else:
            print(f"Запрос по работодателю  {employer_id} не выполнен: {response.status_code}")

    return employers_list

def get_vacancies(page=0) -> list[dict]:
    """Получает с сайта hh.ru данные о вакансиях конкретных работодателей """

    url = "https://api.hh.ru/vacancies"
    params = {
        "page": page,  # номер страницы
        "employer_id": hh_config.get("employer_list"),  # работодатели
        "only_with_salary": hh_config.get("only_with_salary"),  # указана ли зарплата
        "per_page": hh_config.get("per_page"),  # количество вакансий на стр
    }

    # делаем request запрос
    response = requests.get(url, params=params)

    if response.status_code == 200:
        # переводим полученные данные в json формат
        vacancies = response.json()
        vacancies_list = []

        for vacancy in vacancies["items"]:
            # собираем данные по вакансиям в список
            vacancies_list.append({"vacancy_id": vacancy["id"],
                                   "title": vacancy["name"],
                                   "url": vacancy["alternate_url"],
                                   "employer": vacancy["employer"]["id"],
                                   "employer_name": vacancy["employer"]["name"],
                                   "salary_from": vacancy["salary"]["from"],
                                   "salary_to": vacancy["salary"]["to"]})

        return vacancies_list

    else:
        print(f"Запрос не выполнен: {response.status_code}")


def create_database(params, db_name) -> None:
    """Создаем новую базу данных"""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    # Проверяем наличие базы данных
    cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))

    cur.execute(f"DROP DATABASE {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")


    cur.close()
    conn.close()

def create_employers_table(cur) -> None:
    """Создает таблицу с работодателями employers """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employers (
            employer_id int PRIMARY KEY,
            employer_name varchar(100) NOT NULL,
            employer_url varchar(100)
        )
    """),
    cur.execute("""
           ALTER TABLE employers ADD CONSTRAINT unique_employers_name UNIQUE (employer_name)
        """)


def create_vacancies_table(cur) -> None:
    """Создает таблицу с вакансиями vacancies """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vacancies (
            vacancy_id int PRIMARY KEY,
            vacancy_name varchar(100) NOT NULL,
            vacancy_url varchar(100),
            vacancy_employer int REFERENCES employers(employer_id),
            vacancy_employer_name varchar(100) NOT NULL,
            salary_from integer,
            salary_to integer
        )
    """)

def insert_employers_data(cur, employers: list[dict]) -> None:
    """Добавляет данные в таблицу employers """
    for emp in employers:
        cur.execute(
            """
            INSERT INTO employers (employer_id, employer_name, employer_url)
            VALUES (%s, %s, %s)
            """,
            (emp["employer_id"], emp["employer_title"], emp["employer_url"])
        )

def insert_vacancies_data(cur, vacancies: list[dict]) -> None:
    """Добавляет данные в таблицу vacancies """
    for vac in vacancies:
        cur.execute(
            """
            INSERT INTO vacancies (vacancy_id, vacancy_name, vacancy_url, vacancy_employer,vacancy_employer_name, salary_from, salary_to)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (vac["vacancy_id"], vac["title"], vac["url"], vac["employer"], vac["employer_name"], vac["salary_from"], vac["salary_to"])
        )