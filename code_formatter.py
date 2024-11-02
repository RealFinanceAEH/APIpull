import os
import subprocess


def format_with_black(file_path):
    try:
        print(f"Форматирование {file_path} с помощью black...")
        subprocess.run(["black", file_path], check=True)
        print(f"{file_path} успешно отформатирован с помощью black.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка форматирования {file_path} с помощью black: {e}")


def sort_imports_with_isort(file_path):
    try:
        print(f"Сортировка импортов в {file_path} с помощью isort...")
        subprocess.run(["isort", file_path], check=True)
        print(f"Импорты в {file_path} успешно отсортированы с помощью isort.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка сортировки импортов в {file_path} с помощью isort: {e}")


def check_style_with_flake8(file_path):
    try:
        print(f"Проверка стиля кода в {file_path} с помощью flake8...")
        result = subprocess.run(
            ["flake8", file_path], check=False, capture_output=True, text=True
        )
        if result.stdout:
            print(f"Ошибки flake8 в {file_path}:\n{result.stdout}")
        else:
            print(f"Ошибок flake8 в {file_path} нет.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка проверки стиля кода {file_path} с помощью flake8: {e}")


def clean_code(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                format_with_black(file_path)
                sort_imports_with_isort(file_path)
                check_style_with_flake8(file_path)


if __name__ == "__main__":
    directory_to_clean = "./"
    clean_code(directory_to_clean)
