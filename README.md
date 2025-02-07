<h1>API PULL</h1>

<p>simple project to pull data from bank (BNP) to locale database</p>

Installiation and run guide

0. install project

    ```sh
    git clone https://github.com/RealFinanceAEH/RealFinanceFrontend

1. install poetry and python

    ```sh
    curl -sSL https://install.python-poetry.org | python3 -

2. install dependencies

    ```sh
   poetry install
   
3. poetry run 

    ```sh
   poetry run ./api_pull/apipull/main.py
   
3. Optional add

    ```sh
   poetry run ./api_pull/apipull/main.py --start_date <start_date> --end_date <end_date>