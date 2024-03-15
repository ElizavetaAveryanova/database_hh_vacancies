import psycopg2
from config_database import db_config
from utils import get_employers, get_vacancies, create_database
from utils import create_employers_table, create_vacancies_table, insert_employers_data, insert_vacancies_data
from dbmanager import DBManager


def main():
    # название БД
    db_name = 'vacancies_hh_database'

    # из database.ini получаем параметры для работы с БД
    params = db_config()
    conn = None

    # создаем БД
    create_database(params, db_name)

    # обновляем параметры
    params.update({'dbname': db_name})
    try:
        # подключаемся к БД
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                # создаем таблицу employers
                create_employers_table(cur)
                # создаем таблицу vacancies
                create_vacancies_table(cur)

                # по api получаем работодателей
                employers = get_employers()
                # загружаем их таблицу employers
                insert_employers_data(cur, employers)

                # по api получаем вакансии
                vacancies = get_vacancies()
                # загружаем их таблицу vacancies
                insert_vacancies_data(cur, vacancies)

                conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    # запускаем цикл вопросов пользователю
    # создаем экземпляр класса DBManager
    dbm = DBManager(db_name)
    user_answer = None
    while user_answer != 0:
        user_answer = input("""\nВыберите необходимый вам тип информации:
        1 - список всех компаний и количество вакансий у каждой компании
        2 - список всех вакансий с подробными данными
        3 - среднюю стартовую зарплату по вакансиям
        4 - список всех вакансий, у которых стартовая зарплата выше средней
        5 - список всех вакансий, в названии которых содержится ключевое слово
        0 - выйти из программы \n""")
        if user_answer == '1':
            print(dbm.get_companies_and_vacancies_count())
        elif user_answer == '2':
            print(dbm.get_all_vacancies())
        elif user_answer == '3':
            print(dbm.get_avg_salary())
        elif user_answer == '4':
            print(dbm.get_vacancies_with_higher_salary())
        elif user_answer == '5':
            user_word = input("Введите ключевое слово: ")
            print(dbm.get_vacancies_with_keyword(user_word))
        elif user_answer == '0':
            dbm.close_connection()
            break
        else:
            print("Вы ввели неверную команду, попробуйте еще раз")
            continue


if __name__ == '__main__':
    main()

