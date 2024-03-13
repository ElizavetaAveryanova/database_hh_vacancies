import psycopg2
from src.config_database import db_config


class DBManager:
    """Класс для работы с вакансиями из БД Postgres"""

    def __init__(self, db_name):
        """Создание экземпляра класса DBManager"""
        self.params = db_config()
        self.conn = psycopg2.connect(dbname=db_name, **self.params)

    def get_companies_and_vacancies_count(self) -> list:
        """Получает список всех компаний и количество вакансий у каждой компании """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT employer_name, COUNT(*)
                        FROM vacancies
                        JOIN employers ON vacancies.vacancy_employer=employers.employer_id
                        GROUP BY employer_name""")
            return cur.fetchall()

    def get_all_vacancies(self) -> list:
        """Получает список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вак. """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT vacancy_name, vacancy_url, salary_from, salary_to, employer_name
                        FROM vacancies
                        JOIN employers ON vacancies.vacancy_employer=employers.employer_id""")
            return cur.fetchall()

    def get_avg_salary(self) -> tuple:
        """Получает среднюю стартовую зарплату по вакансиям """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT ROUND(AVG(salary_from), 2) as avg_salary
                        FROM vacancies""")
            return cur.fetchone()

    def get_vacancies_with_higher_salary(self) -> list:
        """Получает список всех вакансий, у которых стартовая зарплата выше средней стартовой по всем вакансиям """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT vacancy_name
                        FROM vacancies
                        WHERE salary_from > (SELECT AVG(salary_from) FROM vacancies)""")
            return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword) -> list:
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слово """
        keyword1 = keyword.title()
        keyword2 = keyword.lower()
        with self.conn.cursor() as cur:
            cur.execute(f"""SELECT vacancy_name
                        FROM vacancies
                        WHERE vacancy_name LIKE '%{keyword1}%' OR vacancy_name LIKE '%{keyword2}%'""")
            return cur.fetchall()

    def close_connection(self) -> None:
        """Закрывает connection """
        self.conn.close()