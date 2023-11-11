import cProfile
from  load_employees_from_xml_to_stg_stream import process_large_xml_and_insert_to_db

# Создаем объект cProfile
profiler = cProfile.Profile()

profiler.enable()

# Вызываем вашу функцию (или блок кода), которую вы хотите профилировать
process_large_xml_and_insert_to_db()

# Останавливаем профилирование
profiler.disable()

# Выводим статистику профилирования
profiler.print_stats(sort='cumulative')
