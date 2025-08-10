""" Тут просто собраны константы которые в теории могу пригодиться """

URL = 'https://www.sevsu.ru/univers/shedule/'
cookies = {
    'tmr_lvid': 'bc4a7d9d8bdc96c1fd8f2397a612897f',
    'tmr_lvidTS': '1707853985028',
    '_ym_uid': '1707853985533606229',
    'cf_clearance': 'pbPhm2xMQkbtBaJNzw7YFxgUzuV7xidqOWqV9y4Rt3g-1727690987-1.2.1.1-p9zLGYTTopg8F39PL7Iooyzj67nNYV2oDPtSyGcZkhd6c0HqA34q4D1DQIzqqcEhXYWlx4fz8ydcCOzDh633aTPCfPKc24hsCG01v5IqPSwb4Zlh7H_UKQ4HwHz8KxigGK_1mmU6OqNVrpM6EPd2E3sHwfBBmM1r49EZ3cW1yrZAKl9ZihrOg71iQtgfXK8qhU4dljrjVjUM2rnVzZwUkZQ5M1JWQ.uNq6V5YwwxRG1i2ifg6Q55AGIguVohdvlNe1nVLTH1SSCwPdpu2hmNcU0FVHTaRd6YAmCThBTa_8PIcW8QR7k7R0hRKLC7GeEDBCHXxmN5NJewjGnfFariTyT9USNZ7S.NreqCne1eCOT1U_Z6yO4a63Dmt_yDOAYTI5k0sVOD5QAJxbvYNcUOm6cRe_iFg1Oamp7UIMhWTt3SEc9g3N3nmoqODCrJnAKV',
    '__ddg1_': 'jUiRqCieVhXXfnn5PwnV',
    '_ym_d': '1732103188',
    'BITRIX_SM_PK': 'page',
    '_ym_isad': '2',
    'domain_sid': 'aiIR9qUgo1NsZo5MvHdmY%3A1738843469272',
    'PHPSESSID': 'fRWZHl4TFHW3QfBV6Fx0ERq9qrvu2WEx',
    'tmr_detect': '0%7C1738848902945',
}
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en,ru;q=0.9',
    'cache-control': 'max-age=0',
    # 'cookie': 'tmr_lvid=bc4a7d9d8bdc96c1fd8f2397a612897f; tmr_lvidTS=1707853985028; _ym_uid=1707853985533606229; cf_clearance=pbPhm2xMQkbtBaJNzw7YFxgUzuV7xidqOWqV9y4Rt3g-1727690987-1.2.1.1-p9zLGYTTopg8F39PL7Iooyzj67nNYV2oDPtSyGcZkhd6c0HqA34q4D1DQIzqqcEhXYWlx4fz8ydcCOzDh633aTPCfPKc24hsCG01v5IqPSwb4Zlh7H_UKQ4HwHz8KxigGK_1mmU6OqNVrpM6EPd2E3sHwfBBmM1r49EZ3cW1yrZAKl9ZihrOg71iQtgfXK8qhU4dljrjVjUM2rnVzZwUkZQ5M1JWQ.uNq6V5YwwxRG1i2ifg6Q55AGIguVohdvlNe1nVLTH1SSCwPdpu2hmNcU0FVHTaRd6YAmCThBTa_8PIcW8QR7k7R0hRKLC7GeEDBCHXxmN5NJewjGnfFariTyT9USNZ7S.NreqCne1eCOT1U_Z6yO4a63Dmt_yDOAYTI5k0sVOD5QAJxbvYNcUOm6cRe_iFg1Oamp7UIMhWTt3SEc9g3N3nmoqODCrJnAKV; __ddg1_=jUiRqCieVhXXfnn5PwnV; _ym_d=1732103188; BITRIX_SM_PK=page; _ym_isad=2; domain_sid=aiIR9qUgo1NsZo5MvHdmY%3A1738843469272; PHPSESSID=fRWZHl4TFHW3QfBV6Fx0ERq9qrvu2WEx; tmr_detect=0%7C1738848902945',
    'priority': 'u=0, i',
    'referer': 'https://www.google.com/',
    'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
}
study_forms = {
    "Расписание учебных занятий ОФО, ОЗФО" : 0,
    "Расписание экзаменационной и установочной сессии ЗФО" : 1,
    "Расписание промежуточной аттестации ОФО, ОЗФО" : 2,
}
institutes = {
    "Институт информационных технологий" : 0,
    "Институт ядерной энергии и промышленности" : 1,
    "Юридический институт" : 2,
    "Морской институт" : 3,
    "Институт радиоэлектроники и интеллектуальных технических систем" : 4,
    "Институт финансов, экономики и управления" : 5,
    "Политехнический институт" : 6,
    "Институт общественных наук и международных отношений" : 7,
    "Гуманитарно-педагогический институт" : 8,
    "Институт фундаментальной медицины и здоровьесбережения" : 9,
    "Институт развития города" : 10,
    "Институт перспективных исследований" : 11,
    "Морской колледж" : 12,
    "Аспирантура" : 13,
    "Лицей-предуниверсарий" : 14,
}
semestrs = {
    "I семестр" : 0, 
    "II семестр" : 1
}