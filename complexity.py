from pandas.core.frame import DataFrame

def count_sections(row):
    '''Считает количество разделов.'''
    count = 0

    ps = row['ПС'] == 'Есть'
    os = row['ОС'] == 'Есть'
    soue = row['СОУЭ'] == 'Есть'
    asv = row['Автоматизация систем вентиляции'] == 'Есть'
    characteristics = [ps,os,soue,asv]

    for char in characteristics:
        if char == True:
            count += 1
    return count


def imperator(row) -> bool:
    '''Объекты с трубной разводкой.'''
    if 'Император' in row['Тип оборудования  пожаротушения (Заря/Император)']:
        return True
    return False

def count_types(row) -> int:
    '''Считает количество типов оборудования в проекте.'''
    count = len(list(row['Тип оборудования  пожаротушения (Заря/Император)']))
    return count


def count_modules(row) -> int:
    modules = row['Количество модулей'].strip()
    try:
        count = int(modules)
    except ValueError:
        if '\n' in modules:
            modules = modules.split('\n')
            count = 0
            for integer in modules:
                count += int(integer)
        else:
            return None
    return count

def count_amount_directions_modules(row) ->int:
    amount_dirs = row['Количество направлений'].strip()
    try:
        amount_dirs = int(amount_dirs)
    except ValueError:
        amount_dirs = 0
    count = count_modules(row)
    if count:
        count = count // 20
        amount = count + amount_dirs
        return amount
    return 0



def set_project_complexity(row: DataFrame) -> int:
    '''
    Устанавливает сложность объекта.
    Сложность расчитывается от 1 до 5.
    '''
    amount_sections = count_sections(row)

    has_imperator = imperator(row)

    amount_types = count_types(row)

    amount_dirs = count_amount_directions_modules(row)


    first_type = ['блок-контейнер', 'контейнер', 'школа', 'больница', 'поликлин', 'медицинские учреждения',
                  'мед. учрежд', 'цгс', 'отделение', 'комиссариат', 'гараж']
    second_type = ['адм. здание', 'административное здание', 'жк', 'суд', 'универ']
    third_type = ['производственное здание', 'пром. предприятие', 'выставка', 'музей', 'нии', 'завод']
    fourth_type = ['цод','производственное здание', 'пром. предприятие']

    for type in first_type:
        if type in row['Тип объекта'].strip().lower():
            if amount_sections > 0 or has_imperator == True:
                return 2
            return 1
    for type in second_type:
        if type in row['Тип объекта'].strip().lower():
            if amount_dirs >= 20:
                return 3
            return 2
    for type in third_type:
        if type in row['Тип объекта'].strip().lower():
            if amount_dirs >= 20 or (
                amount_dirs >= 18 and amount_types > 1
            ):
                return 5
            if amount_dirs > 8 or (
                row['Тип оборудования  пожаротушения (Заря/Император)'] == 'ИСТА'
            ):
                return 4
            else:
                return 3
    for type in fourth_type:
        if type in row['Тип объекта'].strip().lower():
            if amount_dirs >= 20 or (
                amount_dirs >= 15 and amount_types > 1
            ):
                return 5
    
    if amount_dirs <= 4:
        return 2
    if amount_dirs <= 15:
        return 3
    elif  amount_types > 1 or amount_sections > 1:
        return 4
    else:
        return 3
